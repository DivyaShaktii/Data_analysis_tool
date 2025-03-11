# backend/core/conversation/response_generator.py
from typing import Dict, List, Optional, Any
import logging

from ...utils.prompt_templates import PROMPT_TEMPLATES

logger = logging.getLogger(__name__)

class ResponseGenerationAgent:
    """
    Agent responsible for generating natural language responses
    based on conversation context and available information.
    """
    
    def __init__(self, llm):
        self.llm = llm
        logger.info("Response Generation Agent initialized")
    
    async def generate(
        self,
        intent: Dict,
        entities: List[Dict],
        conversation_history: List[Dict],
        available_insights: Optional[List[Dict]] = None,
        pending_tasks: Optional[List[Dict]] = None
    ) -> str:
        """
        Generate a natural language response based on the conversation
        context and available information.
        
        Args:
            intent: Identified intent from understanding agent
            entities: Extracted entities from understanding agent
            conversation_history: Previous conversation exchanges
            available_insights: Relevant insights from past analyses
            pending_tasks: Tasks that have been created but not completed
            
        Returns:
            String containing the generated response
        """
        # Format different components for the prompt
        history_text = self._format_history(conversation_history)
        insights_text = self._format_insights(available_insights)
        tasks_text = self._format_tasks(pending_tasks)
        
        # Create the response generation prompt
        prompt = PROMPT_TEMPLATES["response_generation"].format(
            intent=intent["type"],
            entities=self._format_entities(entities),
            conversation_history=history_text,
            available_insights=insights_text,
            pending_tasks=tasks_text
        )
        
        # Generate the response with a slightly higher temperature for natural variation
        response = await self.llm.generate(prompt, temperature=0.7)
        
        logger.info(f"Generated response for intent: {intent['type']}")
        return response.strip()
    
    def _format_history(self, conversation_history: List[Dict]) -> str:
        """Format conversation history for inclusion in the prompt"""
        if not conversation_history:
            return "No prior conversation."
            
        formatted = "Recent conversation history:\n"
        # Only include the last few exchanges to keep prompt size reasonable
        for i, exchange in enumerate(conversation_history[-5:]):
            formatted += f"User: {exchange['message']}\n"
            formatted += f"Assistant: {exchange['response']}\n\n"
        
        return formatted
    
    def _format_insights(self, insights: Optional[List[Dict]]) -> str:
        """Format available insights for inclusion in the prompt"""
        if not insights:
            return "No relevant insights available."
            
        formatted = "Relevant insights from past analyses:\n"
        for i, insight in enumerate(insights):
            formatted += f"{i+1}. {insight['summary']}\n"
            if 'details' in insight:
                formatted += f"   Details: {insight['details']}\n"
        
        return formatted
    
    def _format_tasks(self, tasks: Optional[List[Dict]]) -> str:
        """Format pending tasks for inclusion in the prompt"""
        if not tasks:
            return "No pending analytical tasks."
            
        formatted = "Pending analytical tasks:\n"
        for i, task in enumerate(tasks):
            formatted += f"{i+1}. Task: {task.task_type}\n"
            formatted += f"   Status: {task.status}\n"
            if hasattr(task, 'description') and task.description:
                formatted += f"   Description: {task.description}\n"
        
        return formatted
    
    def _format_entities(self, entities: List[Dict]) -> str:
        """Format extracted entities for inclusion in the prompt"""
        if not entities:
            return "No specific entities identified."
            
        formatted = "Identified entities:\n"
        for entity in entities:
            formatted += f"- {entity['type']}: {entity['value']}\n"
        
        return formatted