from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
from contextlib import asynccontextmanager

# Import routers
from routers import data, chat

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Load models, create connections, etc.
    logger.info("Starting up Analysis Agent server...")
    yield
    # Shutdown: Clean up resources
    logger.info("Shutting down Analysis Agent server...")

# Initialize FastAPI app
app = FastAPI(
    title="Data Analysis Agent API",
    description="Backend API for the Data Analysis Agent",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - restrict this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(data.router)
app.include_router(chat.router)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)