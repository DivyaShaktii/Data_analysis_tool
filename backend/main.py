# backend/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import asyncio

from api.routers import conversation_router, data_router, task_router
#from api.middleware import error_handler, logging_middleware, session_middleware
from core.memory.context_manager import ContextManager
from core.memory.project_store import ProjectStore
from core.task_queue.queue_manager import TaskQueueManager
from core.memory.project_store import ProjectStore
from core.memory.memory_store import MemoryStore
from core.task_queue.task_creator import TaskCreationAgent
from utils.logger import setup_logger

# Initialize the FastAPI app
app = FastAPI(
    title="Data Analyst Agent API",
    description="API for the Agentic Analytics System",
    version="1.0.0"
)

# Set up logging
logger = setup_logger(__name__)

# Initialize core components
session_store = ProjectStore()
context_manager = ContextManager(session_store)
queue_manager = TaskQueueManager()

# Add CORS middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # Adjust for production
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# Commenting out the incorrect middleware addition
# app.add_middleware(context_manager=context_manager)

# Register error handlers
# error_handler.register_exception_handlers(app)

# Include routers
app.include_router(conversation_router.router, prefix="/api/conversation", tags=["conversation"])
app.include_router(data_router.router, prefix="/api/data", tags=["data"])
app.include_router(task_router.router, prefix="/api/task", tags=["task"])

@app.get("/")
async def root():
    return {"message": "Data Analyst Agent API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

def init_memory_and_tasks(app):
    # Create shared components
    project_store = ProjectStore()
    memory_store = MemoryStore()
    
    # Create a default context manager for the request lifecycle
    context_manager = ContextManager(
        project_id="default", 
        user_id="system"
    )
    
    # Create task queue with reference to context manager
    task_queue = TaskQueueManager(
        context_manager=context_manager
    )
    
    # Create task creator with references to both
    task_creator = TaskCreationAgent(
        llm=app.llm_service,
        task_queue=task_queue,
        context_manager=context_manager
    )
    
    # Register them with the application
    app.context_manager = context_manager
    app.task_queue = task_queue
    app.task_creator = task_creator
    
    # Start task processing
    asyncio.create_task(task_queue.start_processing())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)