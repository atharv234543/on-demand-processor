import uuid
import json
import asyncio
import aiomysql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from datetime import datetime, timezone

#database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "your-db-username",
    "password": "your-db-password",
    "db": "your-db-name"
}

#System configuration
MAX_CONCURRENCY = 2
WORKER_COUNT = 2

#Startup and shutdown methods
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, worker_tasks, task_queue, semaphore

    #Creating pool for connections (Startup)
    pool = await aiomysql.create_pool(**DB_CONFIG, minsize=1, maxsize=10)
    for _ in range(WORKER_COUNT):
        t = asyncio.create_task(worker_loop())
        worker_tasks.append(t)
    print("Started workers and DB pool")

    yield

    #Shutting down after the completion
    print("Shutdown started: waiting for tasks to finish...")
    await task_queue.join()
    for t in worker_tasks:
        t.cancel()
    if pool:
        pool.close()
        await pool.wait_closed()
    print("Shutdown finished")


#App initialization
app = FastAPI(title="Job Processor (sleep demo)", lifespan=lifespan)


pool: Optional[aiomysql.Pool] = None
task_queue = asyncio.Queue()
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
worker_tasks: List[asyncio.Task] = []

#Schemas
class TaskIn(BaseModel):
    type: str
    payload: Dict[str, Any] = {}


class JobIn(BaseModel):
    metadata: Dict[str, Any] = {}
    tasks: List[TaskIn]


class TaskOut(BaseModel):
    task_id: str
    job_id: str
    type: str
    state: str
    progress: int
    payload: Dict[str, Any]
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class JobOut(BaseModel):
    job_id: str
    metadata: Dict[str, Any]
    state: str
    overall_progress: float
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    tasks: List[TaskOut] = []


async def get_pool():
    global pool
    if pool is None:
        raise RuntimeError("There is no pool")
    return pool

#Query execution in the database
async def execute(sql: str, params: tuple = (), fetchone=False, fetchall=False, commit=False):
    p = await get_pool()
    async with p.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            if commit:
                await conn.commit()
            if fetchone:
                return await cur.fetchone()
            if fetchall:
                return await cur.fetchall()
            return None


def gen_id(prefix: str):
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def format_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        try:
            # normalize to ISO-8601 string
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)

#Inserting job and tasks
async def insert_job_and_tasks(metadata: Dict[str, Any], tasks: List[TaskIn]):
    """Method to insert metadata of a job and tasks related to that job into the database"""
    
    job_id = gen_id("job")
    await execute(
        "INSERT INTO jobs (job_id,metadata,state) VALUES (%s,%s,'PENDING')",
        (job_id, json.dumps(metadata)),
        commit=True
    )

    inserted_tasks = []
    for t in tasks:
        task_id = gen_id("task")
        await execute(
            "INSERT INTO tasks (task_id, job_id, type, payload, state, progress) VALUES (%s, %s, %s, %s, 'PENDING', 0)",
            (task_id, job_id, t.type, json.dumps(t.payload)),
            commit=True,
        )
        inserted_tasks.append({"task_id": task_id, "job_id": job_id, "type": t.type, "payload": t.payload})

    return job_id, inserted_tasks

#Updating states
async def mark_task_running(task_id: str):
    await execute(
        "UPDATE tasks SET state = 'RUNNING' , started_at = NOW() WHERE task_id = %s",
        (task_id,),
        commit=True,
    )
    row = await execute("SELECT job_id FROM tasks WHERE task_id=%s", (task_id,), fetchone=True)
    if row:
        await execute(
            "UPDATE jobs SET state='RUNNING' WHERE job_id=%s AND state!='RUNNING'",
            (row["job_id"],),
            commit=True
        )


async def mark_task_succeeded(task_id: str):
    await execute(
        "UPDATE tasks SET state='SUCCEEDED', progress=100, finished_at=NOW() WHERE task_id=%s",
        (task_id,),
        commit=True,
    )
    await recompute_job_state_progress_by_task(task_id)


async def mark_task_failed(task_id: str):
    await execute(
        "UPDATE tasks SET state='FAILED', finished_at=NOW() WHERE task_id=%s",
        (task_id,),
        commit=True,
    )
    await recompute_job_state_progress_by_task(task_id)

#Recomputing progress of job by task
async def recompute_job_state_progress_by_task(task_id: str):
    row = await execute("SELECT job_id FROM tasks WHERE task_id=%s", (task_id,), fetchone=True)
    if not row:
        return
    job_id = row["job_id"]

    tasks = await execute("SELECT state, progress FROM tasks WHERE job_id=%s", (job_id,), fetchall=True)
    if not tasks:
        return

    states = [t["state"] for t in tasks]
    progresses = [t["progress"] for t in tasks]
    total = len(progresses)
    overall = round(sum(progresses) / total, 2) if total > 0 else 0.0

    if all(s == 'SUCCEEDED' for s in states):
        job_state = 'COMPLETED'
    elif any(s == 'FAILED' for s in states):
        job_state = 'FAILED'
    elif any(s == 'RUNNING' for s in states):
        job_state = 'RUNNING'
    else:
        job_state = 'PENDING'

    await execute("UPDATE jobs SET state=%s WHERE job_id=%s", (job_state, job_id), commit=True)
    return overall


async def execute_task_sleep(job_id: str, task_id: str, payload: Dict[str, Any]):
    duration = int(payload.get("duration", payload.get("seconds", 3) or 3))
    if duration <= 0:
        duration = 1

    await mark_task_running(task_id)
    try:
        await asyncio.sleep(duration)
        await mark_task_succeeded(task_id)
    except Exception:
        await mark_task_failed(task_id)
        raise


async def run_task_worker(job_id: str, task_id: str, ttype: str, payload: Dict[str, Any]):
    async with semaphore:
        if ttype.lower() == "sleep":
            await execute_task_sleep(job_id, task_id, payload)
        else:
            await mark_task_failed(task_id)


async def worker_loop():
    while True:
        job_id, task_id, ttype, payload = await task_queue.get()
        try:
            await run_task_worker(job_id, task_id, ttype, payload)
        except Exception as e:
            print(f"Error running task {task_id}: {e}")
        finally:
            task_queue.task_done()


def compute_progress_from_row(row: Dict[str, Any]) -> int:
    state = row.get("state")
    stored_progress = row.get("progress") or 0

    if state == "SUCCEEDED":
        return 100

    if state in ("FAILED", "PENDING", "CANCELLED"):
        return int(stored_progress)

    if state == "RUNNING":
        started_at = row.get("started_at")
        payload = {}
        try:
            payload = json.loads(row["payload"]) if row.get("payload") else {}
        except Exception:
            payload = {}
        duration = int(payload.get("duration", payload.get("seconds", 0) or 0))

        if duration <= 0:
            return int(stored_progress)
        if not started_at:
            return int(stored_progress)

        
        
        from datetime import datetime, timezone, timedelta
        
        now = datetime.now(timezone.utc)
        LOCAL_TZ = timezone(timedelta(hours=5, minutes=30))  

        if started_at and started_at.tzinfo is None:
        # Convert naive datetime (DB time) to local timezone then to UTC
            started = started_at.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc)
        else:
            started = started_at

        elapsed = (now - started).total_seconds()
        print(f"Elapses time is : {elapsed}")
        percent = round((elapsed / duration) * 100)

        if percent >= 100:
            return 99
        if percent < 0:
            percent = 0
        print(f"Percentage is : {percent}%")
        return percent

    return int(stored_progress)


@app.post("/jobs", response_model=Dict[str, Any])
async def submit_job(job_in: JobIn):
    job_id, inserted_tasks = await insert_job_and_tasks(job_in.metadata, job_in.tasks)
    for t in inserted_tasks:
        await task_queue.put((t["job_id"], t["task_id"], t["type"], t["payload"]))
    return {"job_id": job_id,"Task ids":[i["task_id"] for i in inserted_tasks]}


@app.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: str):
    job = await execute("SELECT * FROM jobs WHERE job_id=%s", (job_id,), fetchone=True)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    task_rows = await execute("SELECT * FROM tasks WHERE job_id=%s", (job_id,), fetchall=True)
    tasks_out = []
    for r in task_rows:
        progress = compute_progress_from_row(r)
        tasks_out.append(
            TaskOut(
                task_id=r["task_id"],
                job_id=r["job_id"],
                type=r["type"],
                state=r["state"],
                progress=progress,
                payload=json.loads(r["payload"]) if r["payload"] else {},
                started_at=format_ts(r.get("started_at")),
                finished_at=format_ts(r.get("finished_at")),
            )
        )

    total = len(tasks_out)
    overall = round(sum(t.progress for t in tasks_out) / total, 2) if total > 0 else 0.0

    return JobOut(
        job_id=job["job_id"],
        metadata=json.loads(job["metadata"]) if job["metadata"] else {},
        state=job["state"],
        overall_progress=overall,
        created_at=str(job.get("created_at")),
        updated_at=str(job.get("updated_at")),
        tasks=tasks_out,
    )


@app.get("/tasks/{task_id}", response_model=TaskOut)
async def get_task(task_id: str):
    r = await execute("SELECT * FROM tasks WHERE task_id=%s", (task_id,), fetchone=True)
    if not r:
        raise HTTPException(status_code=404, detail="task not found")
    progress = compute_progress_from_row(r)
    return TaskOut(
        task_id=r["task_id"],
        job_id=r["job_id"],
        type=r["type"],
        state=r["state"],
        progress=progress,
        payload=json.loads(r["payload"]) if r["payload"] else {},
        started_at=format_ts(r.get("started_at")),
        finished_at=format_ts(r.get("finished_at")),
    )



