# backend/core/memory/context_manager.py

"""
Context Manager for maintaining conversation state and file context during user interactions.
This module provides a unified interface for managing different memory types:
- Current conversation context (short-term)
- Session-specific memory (medium-term)
- Persistent knowledge (long-term)
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from .session_store import SessionStore
from .memory_store import MemoryStore

logger = logging.getLogger(__name__)

class ContextManager:
    """
    Manages the context of ongoing conversations by integrating different memory systems.
    Provides methods to store, retrieve, and update conversation context, file metadata,
    and analysis results.
    """
    
    def __init__(self, session_id: str):
        """
        Initialize the context manager for a specific user session.
        
        Args:
            session_id: Unique identifier for the user session
        """
        self.session_id = session_id
        self.session_store = SessionStore()
        self.memory_store = MemoryStore()
        
        # Initialize context if it doesn't exist
        if not self.session_store.session_exists(session_id):
            self.initialize_session()
    
    def initialize_session(self) -> None:
        """Create a new session with empty context"""
        self.session_store.create_session(self.session_id, {
            'messages': [],
            'files': {},
            'active_tasks': [],
            'completed_tasks': [],
            'insights': [],
            'session_start': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat()
        })
    
    def add_message(self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a new message to the conversation history.
        
        Args:
            role: The role of the message sender (user/assistant/system)
            content: The message content
            metadata: Optional metadata about the message (e.g., intent, entities)
        """
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        session = self.session_store.get_session(self.session_id)
        session['messages'].append(message)
        session['last_activity'] = datetime.now().isoformat()
        self.session_store.update_session(self.session_id, session)
        
        # Store important insights in long-term memory if this is an assistant response
        if role == 'assistant' and metadata and metadata.get('contains_insight', False):
            self.memory_store.store_insight(
                session_id=self.session_id,
                content=content,
                entities=metadata.get('entities', []),
                context=self._get_recent_context(3)
            )
    
    def add_file(self, file_id: str, metadata: Dict[str, Any]) -> None:
        """
        Add file metadata to the session context.
        
        Args:
            file_id: Unique identifier for the file
            metadata: File metadata including schema, stats, etc.
        """
        session = self.session_store.get_session(self.session_id)
        session['files'][file_id] = {
            'metadata': metadata,
            'added_at': datetime.now().isoformat()
        }
        self.session_store.update_session(self.session_id, session)
        
        # Store file schema in long-term memory for future reference
        self.memory_store.store_file_schema(
            session_id=self.session_id,
            file_id=file_id,
            schema=metadata.get('schema', {}),
            description=metadata.get('description', '')
        )
    
    def add_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Add a new task to the session's active tasks.
        
        Args:
            task_id: Unique identifier for the task
            task_data: Task definition and parameters
        """
        session = self.session_store.get_session(self.session_id)
        session['active_tasks'].append({
            'task_id': task_id,
            'created_at': datetime.now().isoformat(),
            'status': 'pending',
            'data': task_data
        })
        self.session_store.update_session(self.session_id, session)
    
    def update_task_status(self, task_id: str, status: str, results: Optional[Dict[str, Any]] = None) -> None:
        """
        Update the status of a task and optionally add results.
        
        Args:
            task_id: Unique identifier for the task
            status: New status (pending, running, completed, failed)
            results: Optional results data if the task is completed
        """
        session = self.session_store.get_session(self.session_id)
        
        # Find the task in active tasks
        for task in session['active_tasks']:
            if task['task_id'] == task_id:
                task['status'] = status
                task['updated_at'] = datetime.now().isoformat()
                
                if status == 'completed' and results:
                    task['results'] = results
                    session['active_tasks'].remove(task)
                    session['completed_tasks'].append(task)
                    
                    # Store task results in long-term memory
                    self.memory_store.store_analysis_result(
                        session_id=self.session_id,
                        task_id=task_id,
                        task_type=task['data'].get('task_type', ''),
                        entities=task['data'].get('entities', []),
                        results=results
                    )
                    
                elif status == 'failed' and results:
                    task['error'] = results
                
                break
                
        self.session_store.update_session(self.session_id, session)
    
    def add_insight(self, insight: Dict[str, Any]) -> None:
        """
        Add a generated insight to the session.
        
        Args:
            insight: The insight data including content and metadata
        """
        session = self.session_store.get_session(self.session_id)
        insight['timestamp'] = datetime.now().isoformat()
        session['insights'].append(insight)
        self.session_store.update_session(self.session_id, session)
        
        # Store in long-term memory
        self.memory_store.store_insight(
            session_id=self.session_id,
            content=insight['content'],
            entities=insight.get('entities', []),
            context=self._get_recent_context(3)
        )
    
    def get_conversation_history(self, session_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent conversation history.
        
        Args:
            session_id: Current conversation session ID
            limit: Maximum number of messages to return
            
        Returns:
            List of recent messages
        """
        session = self.session_store.get_session(session_id)
        return session['messages'][-limit:] if session['messages'] else []
    
    async def get_recent_history(
        self, 
        user_id: str, 
        session_id: str,
        limit: int = 10
    ) -> List[Dict]:
        """Get recent conversation history"""
        # Use the provided session_id instead of self.session_id
        return self.get_conversation_history(session_id, limit)
    
    async def get_relevant_insights(self, user_id: str, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get insights relevant to the given query.
        
        Args:
            user_id: Unique identifier for the user
            query: Text query to match against stored insights
            limit: Maximum number of insights to return
            
        Returns:
            List of relevant insights
        """
        # Use the memory_store to retrieve relevant insights
        return self.memory_store.retrieve_relevant_insights(
            session_id=self.session_id,
            query=query,
            limit=limit
        )
    
    async def store_interaction(self, user_id: str, session_id: str, interaction_id: str, 
                               message: str, response: str, intent: str, 
                               entities: List[Any], is_followup: bool) -> None:
        """
        Store a user-assistant interaction in memory.
        
        Args:
            user_id: Unique identifier for the user
            session_id: Current conversation session ID
            interaction_id: Unique identifier for this interaction
            message: The user's message
            response: The assistant's response
            intent: Detected intent of the user's message
            entities: Entities extracted from the message
            is_followup: Whether this is a followup question
        """
        # Update session_id to the one provided in the parameters
        self.session_id = session_id
        
        # Add user message
        self.add_message(
            role="user",
            content=message,
            metadata={
                "interaction_id": interaction_id,
                "intent": intent,
                "entities": entities
            }
        )
        
        # Add assistant response
        self.add_message(
            role="assistant",
            content=response,
            metadata={
                "interaction_id": interaction_id,
                "is_followup": is_followup,
                "contains_insight": not is_followup,  # Assume non-followups contain insights
                "entities": entities
            }
        )
    
    def get_conversation_context(self) -> Dict[str, Any]:
        """
        Get the complete context needed for responding to the user.
        
        Returns:
            Dict containing conversation history, files, tasks, and insights
        """
        session = self.session_store.get_session(self.session_id)
        
        # Get relevant insights from long-term memory based on recent context
        recent_messages = self._get_recent_messages_text(3)
        relevant_insights = self.memory_store.retrieve_relevant_insights(
            session_id=self.session_id,
            query=recent_messages,
            limit=5
        )
        
        return {
            'messages': session['messages'],
            'files': session['files'],
            'active_tasks': session['active_tasks'],
            'completed_tasks': session['completed_tasks'][-5:],  # Only recent ones
            'insights': session['insights'] + relevant_insights,
            'session_duration': self._calculate_session_duration(session)
        }
    
    def get_file_context(self) -> Dict[str, Dict]:
        """
        Get context about all files in the current session.
        
        Returns:
            Dict mapping file_ids to their metadata
        """
        session = self.session_store.get_session(self.session_id)
        return session.get('files', {})
    
    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """
        Get all active tasks in the current session.
        
        Returns:
            List of active tasks
        """
        session = self.session_store.get_session(self.session_id)
        return session.get('active_tasks', [])
    
    def get_completed_tasks(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get recently completed tasks.
        
        Args:
            limit: Maximum number of tasks to return
            
        Returns:
            List of completed tasks
        """
        session = self.session_store.get_session(self.session_id)
        return session.get('completed_tasks', [])[-limit:]
    
    def get_insights(self) -> List[Dict[str, Any]]:
        """
        Get all insights generated in the current session.
        
        Returns:
            List of insights
        """
        session = self.session_store.get_session(self.session_id)
        return session.get('insights', [])
    
    def _get_recent_context(self, message_count: int) -> str:
        """Get recent conversation messages as a single string for context"""
        return self._get_recent_messages_text(message_count)
    
    def _get_recent_messages_text(self, count: int) -> str:
        """Extract text from recent messages"""
        messages = self.get_conversation_history(self.session_id, count)
        return "\n".join([f"{m['role']}: {m['content']}" for m in messages])
    
    def _calculate_session_duration(self, session: Dict[str, Any]) -> str:
        """Calculate the duration of the current session"""
        start = datetime.fromisoformat(session['session_start'])
        last = datetime.fromisoformat(session['last_activity'])
        duration = last - start
        return str(duration)
    
    def clear_session(self) -> None:
        """Clear the current session data"""
        self.initialize_session()

    async def session_exists(self, session_id: str) -> bool:
        """Check if a session exists"""
        try:
            self.session_store.get_session(session_id)
            return True
        except KeyError:
            return False
        
    async def create_session(self, session_id: str, user_id: str) -> None:
        """Create a new session"""
        # Create initial data dictionary instead of passing user_id directly
        initial_data = {
            'user_id': user_id,
            'messages': [],
            'files': {},
            'active_tasks': [],
            'completed_tasks': [],
            'insights': [],
            'session_start': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat()
        }
        self.session_store.create_session(session_id, initial_data)
        