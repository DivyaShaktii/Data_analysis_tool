# backend/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os

from api.routers import conversation_router, data_router, task_router
from api.middleware import error_handler, logging_middleware, session_middleware
from core.memory.context_manager import ContextManager
from core.memory.session_store import SessionStore
from core.task_queue.queue_manager import TaskQueueManager
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
session_store = SessionStore()
context_manager = ContextManager(session_store)
queue_manager = TaskQueueManager()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add custom middleware
app.add_middleware(logging_middleware.LoggingMiddleware)
app.add_middleware(session_middleware.SessionMiddleware, context_manager=context_manager)

# Register error handlers
error_handler.register_exception_handlers(app)

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)