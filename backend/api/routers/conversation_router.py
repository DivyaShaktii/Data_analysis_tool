# backend/api/routers/conversation_router.py
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from core.conversation.engine import ConversationEngine
#from ...core.memory.context_manager import ContextManager

router = APIRouter()

class MessageRequest(BaseModel):
    message: str
    user_id :Optional[str] = None
    session_id: Optional[str] = None
    file_context: Optional[ Dict] =  None

class MessageResponse(BaseModel):
    response: str
    session_id: str
    tasks_created: List[Dict[str, Any]] = []


@router.post("/message", response_model=MessageResponse)
async def process_message(
    request: MessageRequest,
    req: Request,
):
    try:
        # Get the context manager from middleware
        context_manager = req.state.context_manager
        
        # Get or create a conversation engine for this session
        
        conversation_engine = ConversationEngine(memory_service=context_manager)
        
        # Process the message
        response_data = await conversation_engine.handle_message(
            message=request.message,
            user_id=request.user_id or "default_user",
            session_id=request.session_id or "default_session",
            file_context=request.file_context
        )
        
        return {
            "response": response_data["message"],
            "session_id": request.session_id or "default_session",
            "tasks_created": response_data.get("pending_tasks", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")

@router.get("/history/{session_id}")
async def get_conversation_history(
    session_id: str,
    req: Request,
):
    try:
        context_manager = req.state.context_manager
        history = context_manager.get_conversation_history(session_id)
        return {"session_id": session_id, "history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving conversation history: {str(e)}")