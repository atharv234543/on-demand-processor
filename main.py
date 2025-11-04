import uuid
import json
import asyncio
import aiomysql
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List,Dict,Any,Optional

DB_CONFIG = {
    "host":"localhost",
    "port": "3030",
    "user" :"root",
    "password":"Mysql@2131",
    "db":"job_db"
}


MAX_CONCURRENCY = 2
WORKER_COUNT = 1

app = FastAPI()

pool : Optional[aiomysql.Pool] = None
task_queue = asyncio.Queue()
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
worker_tasks = List[asyncio.Task] = []


class TaskIn(BaseModel):
    type : str
    payload : Dict[str,Any] = {}

class JobIn(BaseModel):
    metadata : Dict[str,Any] = {}
    tasks : List[TaskIn]
    
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

async def  get_pool():
    global pool
    if pool is None:
        raise RuntimeError("There is no pool")
    return pool

async def execute(sql: str, params:tuple = (), fetchone= False,fetchall = False, commit = False):
    p = await get_pool()
    
    async with p.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql,params)
            if(commit):
                await conn.commit()
            if(fetchone):
                return await cur.fetchone()
            if(fetchall):
                return await cur.fetchall()
            return None
        

#Business logic helpers

def gen_id(prefix : str):
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

async def insert_job_and_tasks(metadata: Dict[str,Any],tasks:List[TaskIn]):
    job_id = gen_id("job")
    
    await execute(
        "INSERT INTO jobs (job_id,metadata,state) VALUES (%s,%s,'PENDING')",
        (job_id,json.dumps(metadata),),
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
        
        inserted_tasks.append({"task_id":task_id,"job_id":job_id,"type":t.type,"payload":t.payload})
    
    return job_id,inserted_tasks


async def mark_task_running(task_id:str):
    await execute(
        "UPDATE tasks SET state = 'RUNNING' , started_at = NOW() WHERE task_id = %s",
        (task_id,),
        commit=True, 
    )
    
    row = await execute("SELECT job_id FROM tasks WHERE task_id=&s",(task_id,),fetchone=True)
    if row:
        await execute("UPDATE jobs SET state 'RUNNING' WHERE job_id=%s AND state!='RUNNING'",
                      (row["job_id"],), commit=True)


async def mark_task_succeeded(task_id: str):
    await execute(
        "UPDATE tasks SET state='SUCCEEDED', progress=100, finished_at=NOW() WHERE task_id=%s",
        (task_id,),
        commit=True,
    )
    # recompute job aggregate
    await recompute_job_state_progress_by_task(task_id)

async def mark_task_failed(task_id: str):
    await execute(
        "UPDATE tasks SET state='FAILED', finished_at=NOW() WHERE task_id=%s",
        (task_id,),
        commit=True,
    )
    await recompute_job_state_progress_by_task(task_id)


async def recompute_job_state_progress_by_task(task_id: str):
    
    row = await execute("SELECT job_id FROM tasks WHERE task_id=%s", (task_id,), fetchone=True)
    if not row:
        return
    job_id = row["job_id"]
    
    tasks = await execute("SELECT state , progress , FROM tasks WHERE job_id=%s",(job_id,), fetchall=True)
    if not tasks:
        return
    
    states = [t["state"] for t in tasks]
    progresses = [t["progress"] for t in tasks]
    total = len(progresses)
    overall= round(sum(progresses) / total , 2) if total>0 else 0.0
    
    if all(s == 'SUCCEEDED' for s in states):
        job_state = 'COMPLETED'
    elif any(s == 'FAILED' for s in states):
        job_state = 'FAILED'
    elif any(s == 'RUNNING' for s in states):
        job_state = 'RUNNING'
    else:
        job_state = 'PENDING'
    
    await execute("UPDATE jobs SET state=%s WHERE job_id=%s", (job_state,job_id), commit=True)
    
    return overall


#Worker
async def execute_task_sleep(job_id: str,task_id:str,payload:Dict[str,Any]):
    """
    Payload includes duration (in seconds) .
    Worker will mark RUNNING at the start and SUCCEEDED at the end.
    Progress (live status) is computed on demand.
    """
    
    duration = int(payload.get("duration", payload.get("seconds",3) or 3))
    if duration <= 0:
        duration = 1
    
    await mark_task_running(task_id)
    
    try:
        
        await asyncio.sleep(duration)
        await mark_task_succeeded(task_id)
    
    except Exception:
        await mark_task_failed(task_id)
        raise

async def run_task_worker(job_id: str,task_id:str,ttype: str,payload:Dict[str,Any]):
    async with semaphore:
        if ttype.lower() == "sleep":
            await execute_task_sleep(job_id,task_id,payload)
        else:
            await mark_task_failed(task_id)

async def worker_loop():
    while True:
        job_id,task_id,ttype,payload = await task_queue.get()
        try:
            await run_task_worker(job_id,task_id,ttype,payload)
        except Exception as e:
            print(f"Error running task {task_id}: {e}")
        finally:
            task_queue.task_done()


#Computing on-demand progress
def compute_progress_from_row(row:Dict[str,Any])-> int:
    state = row.get("state")
    stored_progress = row.get("progress") or 0
    
    if state == "SUCCEEDED":
        return 100
    
    if state in ("FAILED","PENDING","CANCELLED"):
        return int(stored_progress)
    
    if state == "RUNNING":
        started_at = row.get("started_at")
        payload = {}
        try:
            payload = json.loads(row["payload"]) if row.get("payload") else {}
        except Exception:
            payload = {}
        duration = int(payload.get("duration",payload.get("seconds",0) or 0))
        
        if duration<=0:
            return int(stored_progress)
        
        if not started_at:
            return int(stored_progress)
        
        now = datetime.now(timezone.utc)
        
        if isinstance(started_at,datetime) and started_at.tzinfo is None:
            started = started_at.replace(tzinfo=timezone.utc)
        else:
            started = started_at
        elapsed = (now-started).total_seconds()
        percent = int((elapsed/duration) * 100)
        
        if percent >= 100:
            return 99
        if percent < 0:
            percent = 0
        return percent
    
    return int(stored_progress)    

#API-endpoints

@app.post("/jobs",response_model=Dict[str,str])
async def submit_job(job_in: JobIn):
    job_id,inserted_tasks = await insert_job_and_tasks(job_in.metadata,job_in.tasks)
    
    for t in inserted_tasks:
        await task_queue.put((t["job_id"],t["task_id"],t["type"],t["payload"]))
        
    return {"job_id": job_id}

