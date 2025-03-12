# backend/api/routers/task_router.py
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uuid

from core.task_queue.queue_manager import TaskQueueManager
from core.task_queue.task_schema import Task, TaskStatus, TaskType

router = APIRouter()

class TaskRequest(BaseModel):
    session_id: str
    task_type: TaskType
    description: str
    parameters: Dict[str, Any] = {}

class TaskResponse(BaseModel):
    task_id: str
    status: TaskStatus
    message: str

@router.post("", response_model=TaskResponse)
async def create_task(
    task_request: TaskRequest,
    req: Request
):
    try:
        # Create a task
        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            session_id=task_request.session_id,
            task_type=task_request.task_type,
            description=task_request.description,
            parameters=task_request.parameters,
            status=TaskStatus.QUEUED
        )
        
        # Add task to queue
        queue_manager = TaskQueueManager()
        queue_manager.add_task(task)
        
        return {
            "task_id": task_id,
            "status": TaskStatus.QUEUED,
            "message": f"Task created and queued successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating task: {str(e)}")

@router.get("/{task_id}")
async def get_task_status(
    task_id: str,
    req: Request
):
    try:
        queue_manager = TaskQueueManager()
        task = queue_manager.get_task(task_id)
        
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
        return {
            "task_id": task.task_id,
            "status": task.status,
            "description": task.description,
            "created_at": task.created_at,
            "updated_at": task.updated_at,
            "results": task.results if task.results else None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving task: {str(e)}")

@router.get("")
async def list_tasks(
    session_id: str,
    req: Request ,
    status: Optional[TaskStatus] = None
    
):
    try:
        queue_manager = TaskQueueManager()
        tasks = queue_manager.list_tasks(session_id, status)
        
        return {
            "session_id": session_id,
            "count": len(tasks),
            "tasks": tasks
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing tasks: {str(e)}")