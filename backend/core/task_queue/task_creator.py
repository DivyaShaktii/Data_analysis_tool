# backend/core/task_queue/task_creator.py
from typing import Dict, List, Optional, Any
import logging
import uuid
from datetime import datetime

from ...utils.prompt_templates import PROMPT_TEMPLATES
from .task_schema import Task

logger = logging.getLogger(__name__)

class TaskCreationAgent:
    """
    Agent responsible for translating user intents into structured tasks
    for the analytical system to process.
    """
    
    def __init__(self, llm, task_queue):
        self.llm = llm
        self.task_queue = task_queue
        logger.info("Task Creation Agent initialized")
    
    async def create_task(
        self,
        user_id: str,
        session_id: str,
        intent: Dict,
        entities: List[Dict],
        context: List[Dict],
        file_context: Optional[Dict] = None
    ) -> Task:
        """
        Create and enqueue a task based on the user's intent and context.
        
        Args:
            user_id: User identifier
            session_id: Current session identifier
            intent: Identified intent from understanding agent
            entities: Extracted entities from understanding agent
            context: Conversation context
            file_context: Metadata about uploaded files
            
        Returns:
            The created Task object
        """
        # Generate task details using LLM
        task_details = await self._generate_task_details(
            intent=intent,
            entities=entities,
            context=context,
            file_context=file_context
        )
        
        # Create a structured task object
        task = Task(
            id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=session_id,
            created_at=datetime.now().isoformat(),
            status="QUEUED",
            task_type=task_details["task_type"],
            description=task_details.get("description", ""),
            parameters=task_details.get("parameters", {}),
            priority=task_details.get("priority", 1),
            dependencies=task_details.get("dependencies", [])
        )
        
        # Enqueue the task
        await self.task_queue.enqueue(task)
        
        logger.info(f"Created and enqueued task {task.id} of type {task.task_type}")
        return task
    
    async def _generate_task_details(
        self,
        intent: Dict,
        entities: List[Dict],
        context: List[Dict],
        file_context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Use LLM to generate detailed task specifications based on the user's intent.
        
        Args:
            intent: Identified intent
            entities: Extracted entities
            context: Conversation context
            file_context: Metadata about uploaded files
            
        Returns:
            Dict containing task details including type, parameters, etc.
        """
        # Format different components for the prompt
        history_text = self._format_context(context)
        file_context_text = self._format_file_context(file_context)
        entities_text = self._format_entities(entities)
        
        # Create prompt for task generation
        prompt = PROMPT_TEMPLATES["task_creation"].format(
            intent=intent["type"],
            entities=entities_text,
            conversation_context=history_text,
            file_context=file_context_text
        )
        
        # Generate task details
        response = await self.llm.generate(prompt, temperature=0.2)
        
        # Parse the response into structured task details
        try:
            task_details = self._parse_task_response(response)
            return task_details
        except Exception as e:
            logger.error(f"Error parsing task response: {str(e)}")
            # Fall back to a basic task structure
            return {
                "task_type": self._map_intent_to_task_type(intent["type"]),
                "description": f"Analysis based on {intent['type']}",
                "parameters": {"entities": [e["value"] for e in entities]},
                "priority": 1
            }
    
    def _format_context(self, context: List[Dict]) -> str:
        """Format conversation context for the task creation prompt"""
        if not context:
            return "No prior conversation."
            
        formatted = "Recent conversation context:\n"
        for i, exchange in enumerate(context[-3:]):  # Last 3 exchanges
            formatted += f"User: {exchange['message']}\n"
            formatted += f"Assistant: {exchange['response']}\n\n"
        
        return formatted
    
    def _format_file_context(self, file_context: Optional[Dict]) -> str:
        """Format file context for the task creation prompt"""
        if not file_context:
            return "No files available for analysis."
            
        formatted = "Available data files:\n"
        for file_name, metadata in file_context.items():
            formatted += f"- {file_name}: {metadata['type']}, {metadata['size']} bytes\n"
            if 'schema' in metadata:
                formatted += f"  Schema: {metadata['schema']}\n"
                
        return formatted
    
    def _format_entities(self, entities: List[Dict]) -> str:
        """Format extracted entities for the task creation prompt"""
        if not entities:
            return "No specific entities identified."
            
        formatted = "Identified entities:\n"
        for entity in entities:
            formatted += f"- {entity['type']}: {entity['value']}\n"
        
        return formatted
    
    def _parse_task_response(self, response: str) -> Dict[str, Any]:
        """
        Parse the LLM's task creation response into a structured format.
        
        This implementation assumes the LLM returns structured data.
        In practice, you would use a more robust parsing mechanism or
        structure the prompt to ensure consistent formatting.
        """
        # Simple parsing logic for demonstration
        lines = response.strip().split('\n')
        task_details = {}
        
        for line in lines:
            if line.startswith("TASK_TYPE:"):
                task_details["task_type"] = line.replace("TASK_TYPE:", "").strip()
            elif line.startswith("DESCRIPTION:"):
                task_details["description"] = line.replace("DESCRIPTION:", "").strip()
            elif line.startswith("PRIORITY:"):
                try:
                    task_details["priority"] = int(line.replace("PRIORITY:", "").strip())
                except ValueError:
                    task_details["priority"] = 1
        
        # Extract parameters section
        parameters = {}
        param_section = False
        for line in lines:
            if line.startswith("PARAMETERS:"):
                param_section = True
                continue
            if param_section and line.strip() and not line.startswith("---"):
                parts = line.split(':')
                if len(parts) >= 2:
                    key = parts[0].strip()
                    value = ':'.join(parts[1:]).strip()
                    parameters[key] = value
        
        task_details["parameters"] = parameters
        
        return task_details
    
    def _map_intent_to_task_type(self, intent_type: str) -> str:
        """Map intent types to appropriate task types"""
        intent_task_mapping = {
            "data_analysis": "general_analysis",
            "visualization": "data_visualization",
            "summary": "data_summary",
            "prediction": "predictive_model",
            "correlation": "correlation_analysis",
            "comparison": "comparative_analysis",
            "time_series": "time_series_analysis"
        }
        
        return intent_task_mapping.get(intent_type, "general_analysis")