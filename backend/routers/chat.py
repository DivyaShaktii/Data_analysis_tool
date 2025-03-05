from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Dict, Any, Optional, Union
import logging
import uuid
from pydantic import BaseModel

from services.analysis import AnalysisService

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)
analysis_service = AnalysisService()

# Chat message models
class Message(BaseModel):
    role: str  # "user" or "assistant"
    content: str
    message_id: Optional[str] = None

class ChatRequest(BaseModel):
    session_id: str
    message: str
    message_id: Optional[str] = None

class ChatResponse(BaseModel):
    message: Message
    code: Optional[str] = None
    insights: Optional[List[Dict[str, Any]]] = None

# Store conversations (in memory for now, would use proper DB in production)
conversations = {}

@router.post("/message", response_model=ChatResponse)
async def send_message(request: ChatRequest):
    """Send a message to the analysis agent"""
    session_id = request.session_id
    
    # Initialize conversation if it doesn't exist
    if session_id not in conversations:
        conversations[session_id] = []
    
    # Add user message to history
    user_message_id = request.message_id or str(uuid.uuid4())
    user_message = Message(
        role="user",
        content=request.message,
        message_id=user_message_id
    )
    conversations[session_id].append(user_message.dict())
    
    try:
        # Process the message and generate a response
        response_data = await analysis_service.process_message(
            session_id=session_id,
            message=request.message,
            conversation_history=conversations[session_id]
        )
        
        # Create assistant message
        assistant_message = Message(
            role="assistant",
            content=response_data["response"],
            message_id=str(uuid.uuid4())
        )
        
        # Add to conversation history
        conversations[session_id].append(assistant_message.dict())
        
        # Prepare response with optional code and insights
        response = ChatResponse(
            message=assistant_message,
            code=response_data.get("code"),
            insights=response_data.get("insights")
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")

@router.get("/history/{session_id}")
async def get_chat_history(session_id: str):
    """Get the chat history for a session"""
    if session_id not in conversations:
        return {"messages": []}
    
    return {"messages": conversations[session_id]}

@router.delete("/history/{session_id}")
async def clear_chat_history(session_id: str):
    """Clear the chat history for a session"""
    if session_id in conversations:
        conversations[session_id] = []
    
    return {"status": "cleared"}