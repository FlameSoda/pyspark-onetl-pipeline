import os
import uuid
import json
from tkinter.constants import PROJECTING
import yaml
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from src.etl_pipeline import PaySimPipeline
load_dotenv()

app = FastAPI(
    title="PaySim ETL Service",
    description="REST API for managing Spark-based ETL tasks",
    version="1.0.0"
)

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(PROJECT_PATH, "config.yaml")
HISTORY_PATH = os.path.join(PROJECT_PATH, "data", "history")

os.makedirs(HISTORY_PATH, exist_ok=True)
with open(CONFIG_PATH, mode="r", encoding="UTF-8") as rfile:
    config = yaml.safe_load(rfile)

tasks_registry = {}


def run_etl_worker(task_id: str, is_incremental: bool):
    """Background worker that executes the Spark ETL pipeline.
    Updates task status and persists metrics to the history directory"""
    tasks_registry[task_id]["status"] = "running"
    try:
        pipeline = PaySimPipeline(config)
        start_time = datetime.now()
        pipeline.run(is_incremental=is_incremental)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            "task_id": task_id,
            "status": "completed",
            "type": "incremental" if is_incremental else "full",
            "execution_time_sec": round(duration, 2),
            "finished_at": end_time.isoformat()
        }
        tasks_registry[task_id].update(result)
        with open(os.path.join(HISTORY_PATH, f"{task_id}.json"), "w") as f:
            json.dump(result, f)

    except Exception as e:
        tasks_registry[task_id]["status"] = "failed"
        tasks_registry[task_id]["error"] = str(e)


@app.post("/etl/full")
async def start_full_etl(background_tasks: BackgroundTasks):
    """Triggers a full snapshot ETL task"""
    task_id = str(uuid.uuid4())
    tasks_registry[task_id] = {"status": "pending", "type": "full"}
    background_tasks.add_task(run_etl_worker, task_id, False)
    return {"task_id": task_id, "status": "accepted"}


@app.post("/etl/incremental")
async def start_incremental_etl(background_tasks: BackgroundTasks):
    """Triggers an incremental load ETL task"""
    task_id = str(uuid.uuid4())
    tasks_registry[task_id] = {"status": "pending", "type": "incremental"}
    background_tasks.add_task(run_etl_worker, task_id, True)
    return {"task_id": task_id, "status": "accepted"}


@app.get("/etl/status/{task_id}")
async def get_task_status(task_id: str):
    """Monitors the execution status of a specific task"""
    if task_id not in tasks_registry:
        history_file = os.path.join(HISTORY_PATH, f"{task_id}.json")
        if os.path.exists(history_file):
            with open(history_file, "r") as f:
                return json.load(f)
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks_registry[task_id]


@app.get("/etl/history")
async def get_etl_history():
    """Retrieves the history of all executed ETL tasks from storage"""
    history = []
    if os.path.exists(HISTORY_PATH):
        for filename in os.listdir(HISTORY_PATH):
            if filename.endswith(".json"):
                with open(os.path.join(HISTORY_PATH, filename), "r") as f:
                    history.append(json.load(f))
    return history