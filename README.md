On-Demand Processor

Minimal job processor with FastAPI + MySQL. Submit jobs with one or more tasks, track live status and progress, and view results via a simple UI or Swagger.

Requirements
- Python 3.10+
- MySQL 
- Install deps:
  ```bash
  python -m pip install fastapi uvicorn aiomysql pydantic
  ```

Database setup
1) Configure connection in `on-demand-processor/main.py` under `DB_CONFIG` (host, port, user, password, db). Default MySQL port is 3306.
2) Create database and tables:
```sql
CREATE DATABASE IF NOT EXISTS job_db;
USE job_db;

CREATE TABLE IF NOT EXISTS jobs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  job_id VARCHAR(64) UNIQUE NOT NULL,
  metadata JSON NULL,
  state VARCHAR(32) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tasks (
  id INT AUTO_INCREMENT PRIMARY KEY,
  task_id VARCHAR(64) UNIQUE NOT NULL,
  job_id VARCHAR(64) NOT NULL,
  type VARCHAR(64) NOT NULL,
  payload JSON NULL,
  state VARCHAR(32) NOT NULL,
  progress INT NOT NULL DEFAULT 0,
  started_at DATETIME NULL,
  finished_at DATETIME NULL,
  FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);
```

How to run
- API + UI (frontend):
  ```bash
  python -m uvicorn serve:app --app-dir on-demand-processor --reload --lifespan on
  ```
  - UI: `http://127.0.0.1:8000/`
  

- Swagger UI:
  ```bash
  python -m uvicorn main:app --app-dir on-demand-processor --reload --lifespan on
  ```
  - Swagger: `http://127.0.0.1:8000/docs`

Core endpoints
- Submit job (returns job and task IDs)
  - `POST /ui/jobs` body:
    ```json
    { "metadata": {}, "tasks": [{ "type": "sleep", "payload": { "duration": 3 } }] }
    ```
  - response: `{ "job_id": "...", "task_ids": ["task_..."] }`

- Submit job (API): `POST /jobs` → `{ "job_id": "..." }`
- Get job: `GET /jobs/{job_id}` → job + tasks with progress
- Get task: `GET /tasks/{task_id}` → single task with progress
”.

