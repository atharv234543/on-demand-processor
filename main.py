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

    
