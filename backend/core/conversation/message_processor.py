# backend/core/conversation/message_processor.py
from typing import Dict, List, Optional, Any
import logging

from utils.prompt_templates import PROMPT_TEMPLATES

logger = logging.getLogger(__name__)

class UnderstandingAgent:
    """
    Agent responsible for analyzing user messages, identifying intents,
    extracting entities, and determining when clarification is needed.
    """
    
    def __init__(self, llm):
        self.llm = llm
        logger.info("Understanding Agent initialized")
    
    async def analyze(
        self, 
        message: str, 
        conversation_history: List[Dict],
        file_context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Analyze a user message to determine intent, extract entities,
        and decide if follow-up questions are needed.
        
        Args:
            message: The user's message
            conversation_history: List of prior conversation exchanges
            file_context: Optional metadata about uploaded files
            
        Returns:
            Dict containing analysis results including intent, entities,
            and whether clarification is needed
        """
        # Construct prompt for the LLM
        history_text = self._format_history(conversation_history)
        file_context_text = self._format_file_context(file_context)
        
        prompt = PROMPT_TEMPLATES["understanding_agent"].format(
            message=message,
            conversation_history=history_text,
            file_context=file_context_text
        )
        
        # Get analysis from LLM
        response = await self.llm.generate(prompt, temperature=0.2)
        
        # Parse LLM response
        try:
            analysis = self._parse_analysis_response(response)
            logger.info(f"Message analyzed with intent: {analysis.get('intent', {}).get('type', 'unknown')}")
            return analysis
        except Exception as e:
            logger.error(f"Error parsing analysis response: {str(e)}")
            # Fall back to basic analysis
            return {
                "intent": {"type": "general_query", "confidence": 0.6},
                "entities": [],
                "needs_clarification": False
            }
    
    def _format_history(self, conversation_history: List[Dict]) -> str:
        """Format conversation history for inclusion in the prompt"""
        if not conversation_history:
            return "No prior conversation."
            
        formatted = "Prior conversation:\n"
        for i, exchange in enumerate(conversation_history):
            # Safely access 'message' and 'response' keys
            user_message = exchange.get('message', 'No message available')
            assistant_response = exchange.get('response', 'No response available')
            formatted += f"User: {user_message}\n"
            formatted += f"Assistant: {assistant_response}\n\n"
        
        return formatted
    
    def _format_file_context(self, file_context: Optional[Dict]) -> str:
        """Format file context for inclusion in the prompt"""
        if not file_context:
            return "No files uploaded."
            
        formatted = "Uploaded files:\n"
        for file_name, metadata in file_context.items():
            formatted += f"- {file_name}: {metadata['type']}, {metadata['size']} bytes\n"
            if 'schema' in metadata:
                formatted += f"  Schema: {metadata['schema']}\n"
                
        return formatted
    
    def _parse_analysis_response(self, response: str) -> Dict[str, Any]:
        """
        Parse the LLM's response into a structured analysis result.
        
        This implementation assumes the LLM returns JSON-like structured data.
        In practice, you may need more robust parsing logic or structure the
        prompt to ensure consistent formatting.
        """
        # This is a simplified implementation
        # In a real system, use a more robust JSON parsing approach
        # or structure the prompt to return cleaner formats
        
        if "NEEDS_CLARIFICATION: TRUE" in response.upper():
            # Extract the follow-up question
            followup_lines = [line for line in response.split('\n') 
                             if line.startswith("FOLLOWUP:")]
            followup = followup_lines[0].replace("FOLLOWUP:", "").strip() if followup_lines else "Could you provide more details?"
            
            return {
                "intent": {"type": "unclear", "confidence": 0.9},
                "entities": [],
                "needs_clarification": True,
                "followup_question": followup
            }
        
        # Extract intent
        intent_lines = [line for line in response.split('\n') 
                       if line.startswith("INTENT:")]
        intent_type = intent_lines[0].replace("INTENT:", "").strip() if intent_lines else "general_query"
        
        # Check if this requires data analysis
        requires_analysis = "REQUIRES_ANALYSIS: TRUE" in response.upper()
        
        # Extract entities (simplified)
        entities = []
        entity_section = False
        for line in response.split('\n'):
            if line.startswith("ENTITIES:"):
                entity_section = True
                continue
            if entity_section and line.strip() and not line.startswith("---"):
                parts = line.split(':')
                if len(parts) >= 2:
                    entity_type = parts[0].strip()
                    entity_value = ':'.join(parts[1:]).strip()
                    entities.append({"type": entity_type, "value": entity_value})
        
        return {
            "intent": {
                "type": intent_type,
                "confidence": 0.8  # In practice, this would come from the LLM
            },
            "entities": entities,
            "needs_clarification": False,
            "requires_analysis": requires_analysis
        }