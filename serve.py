import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import Dict, Any

# Import the existing API app (do not modify main.py)
from main import app as api_app  # type: ignore
from main import JobIn, insert_job_and_tasks, task_queue  # type: ignore


app: FastAPI = api_app

BASE_DIR = os.path.dirname(__file__)
STATIC_DIR = os.path.join(BASE_DIR, "static")
INDEX_PATH = os.path.join(STATIC_DIR, "index.html")

# We are using the original app directly so its lifespan (DB pool, workers) runs

# Enable CORS for local dev tools if accessed from other ports
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (served from /static) only if the directory exists
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR, html=True), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    try:
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except Exception:
        return HTMLResponse("<h1>Frontend missing</h1><p>Create static/index.html</p>", status_code=200)



@app.post("/ui/jobs", response_model=Dict[str, Any])
async def submit_job_with_task_ids(job_in: JobIn):
    # Create job and tasks using the existing business logic
    job_id, inserted_tasks = await insert_job_and_tasks(job_in.metadata, job_in.tasks)
    # Enqueue tasks for workers
    for t in inserted_tasks:
        await task_queue.put((t["job_id"], t["task_id"], t["type"], t["payload"]))
    # Return job_id and list of task_ids
    task_ids = [t["task_id"] for t in inserted_tasks]
    return {"job_id": job_id, "task_ids": task_ids}

