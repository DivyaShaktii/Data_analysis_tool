# backend/api/middleware/session_middleware.py
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import logging
from core.memory.context_manager import ContextManager

logger = logging.getLogger(__name__)

class SessionMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, context_manager: ContextManager):
        super().__init__(app)
        self.context_manager = context_manager
        
    async def dispatch(self, request: Request, call_next):
        # Make context manager available in request state
        request.state.context_manager = self.context_manager
        
        # Extract session_id from headers or query params if present
        session_id = request.headers.get("X-Session-ID")
        if not session_id and "session_id" in request.query_params:
            session_id = request.query_params["session_id"]
            
        # Store session ID in request state
        if session_id:
            request.state.session_id = session_id
            logger.debug(f"Request associated with session: {session_id}")
        
        response = await call_next(request)
        return response