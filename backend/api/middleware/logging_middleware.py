# backend/api/middleware/logging_middleware.py
from fastapi import Request
import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Log request details
        logger.info(f"Request started: {request.method} {request.url.path}")
        
        # Process the request and get the response
        response = await call_next(request)
        
        # Calculate and log processing time
        process_time = time.time() - start_time
        logger.info(f"Request completed: {request.method} {request.url.path} - Took {process_time:.4f}s - Status: {response.status_code}")
        
        return response