# backend/core/conversation/engine.py
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import uuid

from .message_processor import UnderstandingAgent
from .response_generator import ResponseGenerationAgent
from ..task_queue.task_creator import TaskCreationAgent
from ..memory.context_manager import ContextManager  
from ..task_queue.queue_manager import TaskQueueManager
from utils.prompt_templates import PROMPT_TEMPLATES
from utils.llm_connector import LLMProvider

logger = logging.getLogger(__name__)

class ConversationEngine:
    """Central orchestrator for user interactions"""
    
    def __init__(
        self, 
        llm_provider: str = "openai",
        model_name: str = None,
        memory_service: Optional[ContextManager] = None,
        task_queue: Optional[TaskQueueManager] = None
    ):
        # Initialize LLM provider based on user preference
        self.llm = LLMProvider.create(provider=llm_provider, model_name=model_name)
        
        # Initialize supporting services or use injected ones
        self.memory_service = memory_service or ContextManager()
        self.task_queue = task_queue or TaskQueueManager()
        
        # Initialize specialized agents
        self.understanding_agent = UnderstandingAgent(llm=self.llm)
        self.response_agent = ResponseGenerationAgent(llm=self.llm)
        self.task_agent = TaskCreationAgent(llm=self.llm, task_queue=self.task_queue)
        
        logger.info(f"Conversation Engine initialized with {llm_provider} provider")
    
    async def handle_message(
        self, 
        message: str, 
        user_id: str, 
        session_id: str,
        file_context: Dict = None
    ) -> Dict[str, Any]:
        """
        Process an incoming user message and generate an appropriate response.
        
        Args:
            message: The user's message text
            user_id: Unique identifier for the user
            session_id: Current conversation session ID
            file_context: Optional metadata about uploaded files
            
        Returns:
            Dict containing response type and message content
        """
        logger.info(f"Processing message for user {user_id}: {message[:50]}...")
        
        # Create a unique interaction ID for this exchange
        interaction_id = str(uuid.uuid4())
        
        try:
            # 1. Build context from memory
            conversation_history = await self.memory_service.get_recent_history(
                user_id=user_id, 
                session_id=session_id,
                limit=10
            )
            
            past_insights = await self.memory_service.get_relevant_insights(
                user_id=user_id, 
                query=message
            )
            
            # 2. Analyze message for intent and entities
            analysis_result = await self.understanding_agent.analyze(
                message=message,
                conversation_history=conversation_history,
                file_context=file_context
            )
            
            intent = analysis_result["intent"]
            entities = analysis_result["entities"]
            
            # 3. Determine if clarification is needed
            if analysis_result.get("needs_clarification", False):
                followup_question = analysis_result["followup_question"]
                
                # Store this interaction in memory
                await self.memory_service.store_interaction(
                    user_id=user_id,
                    session_id=session_id,
                    interaction_id=interaction_id,
                    message=message,
                    response=followup_question,
                    intent=intent,
                    entities=entities,
                    is_followup=True
                )
                
                return {
                    "response_type": "followup",
                    "message": followup_question,
                    "interaction_id": interaction_id
                }
            
            # 4. Create tasks if analysis is required
            pending_tasks = []
            if analysis_result.get("requires_analysis", False):
                task = await self.task_agent.create_task(
                    user_id=user_id,
                    session_id=session_id,
                    intent=intent,
                    entities=entities,
                    context=conversation_history,
                    file_context=file_context
                )
                
                logger.info(f"Created task {task.id} for user {user_id}")
                pending_tasks = [task]
            
            # 5. Generate response
            response = await self.response_agent.generate(
                intent=intent,
                entities=entities,
                conversation_history=conversation_history,
                available_insights=past_insights,
                pending_tasks=pending_tasks
            )
            
            # 6. Update memory
            await self.memory_service.store_interaction(
                user_id=user_id,
                session_id=session_id,
                interaction_id=interaction_id,
                message=message,
                response=response,
                intent=intent,
                entities=entities,
                is_followup=False
            )
            
            return {
                "response_type": "standard",
                "message": response,
                "interaction_id": interaction_id,
                "pending_tasks": [t.id for t in pending_tasks] if pending_tasks else []
            }
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            return {
                "response_type": "error",
                "message": "I'm sorry, I encountered an error processing your request. Please try again.",
                "interaction_id": interaction_id
            }