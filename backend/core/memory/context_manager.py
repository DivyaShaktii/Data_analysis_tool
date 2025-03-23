# backend/core/memory/context_manager.py

"""
Context Manager for maintaining conversation state and file context during user interactions.
This module provides a unified interface for managing different memory types:
- Current conversation context (short-term)
- Project-specific memory (medium-term)
- Persistent knowledge across projects (long-term)
"""

from utils.logger import setup_logger
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
from .project_store import ProjectStore
from .memory_store import MemoryStore

logger = setup_logger(__name__)

class ContextManager:
    """
    Manages the context of ongoing conversations by integrating different memory systems.
    Provides methods to store, retrieve, and update conversation context, file metadata,
    and analysis results within a project-centered architecture.
    """
    
    def __init__(self, project_id: str, session_id: Optional[str] = None, user_id: Optional[str] = None):
        """
        Initialize the context manager for a specific project.
        
        Args:
            project_id: Unique identifier for the project
            session_id: Optional identifier for the current session within the project
            user_id: Optional identifier for the user who owns the project
        """
        self.project_id = project_id
        self.session_id = session_id
        self.user_id = user_id
        
        self.project_store = ProjectStore()
        self.memory_store = MemoryStore()
        
        # Initialize project if it doesn't exist
        if user_id and not self.project_store.project_exists(project_id):
            self.initialize_project()
            
        # If session_id is provided, ensure it exists
        if session_id and not self.project_store.session_exists(project_id, session_id):
            self.create_session(project_id , session_id)
    
    def initialize_project(self) -> None:
        """Create a new project with empty context"""
        if not self.user_id:
            raise ValueError("Cannot initialize project without user_id")
            
        self.project_store.create_project(self.project_id, self.user_id, {
            'created_at': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat(),
            'sessions': {},
            'files': {},
            'active_tasks': [],
            'completed_tasks': [],
            'insights': [],
            'code': {},
            'variables': {}
        })
        
        logger.info(f"Initialized new project {self.project_id} for user {self.user_id}")
    
    def create_session(self, session_id: Optional[str] = None) -> str:
        """
        Create a new session within the current project.
        
        Args:
            session_id: Optional specific session ID, generated if not provided
            
        Returns:
            The session ID
        """
        if not session_id:
            session_id = str(uuid.uuid4())
            
        # Make sure project exists before creating a session
        if not self.project_store.project_exists(self.project_id):
            if self.user_id:
                self.initialize_project()
            else:
                raise ValueError(f"Project {self.project_id} does not exist and user_id not provided to create it")
            
        self.project_store.create_session(self.project_id, session_id, {
            'messages': [],
            'session_start': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat()
        })
        
        self.session_id = session_id
        return session_id
    
    # Conversation methods
    
    def add_message(self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a message to the current session.
        
        Args:
            role: The role of the message sender (user/assistant/system)
            content: The message content
            metadata: Optional metadata about the message
        """
        if not self.session_id:
            self.create_session()
            
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        try:
            session = self.project_store.get_session(self.project_id, self.session_id)
            
            # Ensure messages list exists
            if 'messages' not in session:
                session['messages'] = []
                
            session['messages'].append(message)
            session['last_activity'] = datetime.now().isoformat()
            
            # Update session
            self.project_store.update_session(self.project_id, self.session_id, session)
            
            # Update project last activity
            project = self.project_store.get_project(self.project_id)
            project['last_activity'] = datetime.now().isoformat()
            self.project_store.update_project(self.project_id, project)
        
            # Store important insights in long-term memory if this is an assistant response
            if role == 'assistant' and metadata and metadata.get('contains_insight', False):
                self.memory_store.store_insight(
                    project_id=self.project_id,
                session_id=self.session_id,
                content=content,
                entities=metadata.get('entities', []),
                context=self._get_recent_context(3)
            )
                
        except KeyError as e:
            logger.error(f"Failed to add message: {str(e)}")
            # Create session if it doesn't exist
            if not self.project_store.session_exists(self.project_id, self.session_id):
                self.create_session(self.session_id)
                # Try again with the new session
                self.add_message(role, content, metadata)
    
    def get_conversation_history(self, session_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent conversation history.
        
        Args:
            session_id: Specific session ID (uses current session if None)
            limit: Maximum number of messages to return
            
        Returns:
            List of recent messages
        """
        try:
            target_session_id = session_id or self.session_id
            if not target_session_id:
                return []
                
            session = self.project_store.get_session(self.project_id, target_session_id)
            messages = session.get('messages', [])
            return messages[-limit:] if messages else []
        except KeyError:
            return []
    
    # File methods
    
    def add_file(self, file_id: str, metadata: Dict[str, Any]) -> None:
        """
        Add file metadata to the project.
        
        Args:
            file_id: Unique identifier for the file
            metadata: File metadata including schema, stats, etc.
        """
        project = self.project_store.get_project(self.project_id)
        
        # Ensure files dict exists
        if 'files' not in project:
            project['files'] = {}
            
        project['files'][file_id] = {
            'metadata': metadata,
            'added_at': datetime.now().isoformat(),
            'session_id': self.session_id  # Track which session added this file
        }
        
        project['last_activity'] = datetime.now().isoformat()
        self.project_store.update_project(self.project_id, project)
        
        # Store file schema in long-term memory for future reference
        self.memory_store.store_file_schema(
            project_id=self.project_id,
            file_id=file_id,
            schema=metadata.get('schema', {}),
            description=metadata.get('description', '')
        )
    
    def remove_file(self, file_id: str) -> None:
        """
        Remove a file from the project.
        
        Args:
            file_id: Unique identifier for the file to remove
        """
        project = self.project_store.get_project(self.project_id)
        
        if 'files' in project and file_id in project['files']:
            del project['files'][file_id]
            project['last_activity'] = datetime.now().isoformat()
            self.project_store.update_project(self.project_id, project)
            logger.info(f"Removed file {file_id} from project {self.project_id}")
    
    def get_file_context(self) -> Dict[str, Dict]:
        """
        Get context about all files in the current project.
        
        Returns:
            Dict mapping file_ids to their metadata
        """
        project = self.project_store.get_project(self.project_id)
        return project.get('files', {})
    
    # Task methods
    
    def add_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Add a new task to the project's active tasks.
        
        Args:
            task_id: Unique identifier for the task
            task_data: Task definition and parameters
        """
        project = self.project_store.get_project(self.project_id)
        
        # Ensure active_tasks list exists
        if 'active_tasks' not in project:
            project['active_tasks'] = []
        
        # Create task record
        task = {
            'task_id': task_id,
            'created_at': datetime.now().isoformat(),
            'status': 'queued',  # Start with queued status
            'type': task_data.get('task_type'),
            'description': task_data.get('description'),
            'parameters': task_data.get('parameters', {}),
            'data': {  # Structured data field
                'task_type': task_data.get('task_type'),
                'entities': task_data.get('entities', [])
            },
            'session_id': self.session_id,
            'priority': task_data.get('priority', 1)
        }
        
        # Add to active tasks
        project['active_tasks'].append(task)
        project['last_activity'] = datetime.now().isoformat()
        
        # Update project
        self.project_store.update_project(self.project_id, project)
        logger.info(f"Added task {task_id} to project {self.project_id} active tasks")
    
    def update_task_status(self, task_id: str, status: str, results: Optional[Dict[str, Any]] = None) -> None:
        """
        Update the status of a task and optionally add results.
        
        Args:
            task_id: Unique identifier for the task
            status: New status (pending, running, completed, failed)
            results: Optional results data if the task is completed
        """
        project = self.project_store.get_project(self.project_id)
        
        # Find the task in active tasks
        task_found = False
        for task in project.get('active_tasks', []):
            if task['task_id'] == task_id:
                task_found = True
                task['status'] = status
                task['updated_at'] = datetime.now().isoformat()
                
                if status == 'completed' and results:
                    # Create a complete task record
                    completed_task = {
                        'task_id': task_id,
                        'status': status,
                        'type': results.get('type'),
                        'description': results.get('description'),
                        'parameters': results.get('parameters', {}),
                        'created_at': results.get('created_at') or task.get('created_at'),
                        'completed_at': datetime.now().isoformat(),
                        'data': results.get('data') or task.get('data', {}),
                        'results': results.get('results', {}),
                        'session_id': task.get('session_id')
                    }
                    
                    # Remove from active tasks
                    project['active_tasks'].remove(task)
                    
                    # Ensure completed_tasks list exists
                    if 'completed_tasks' not in project:
                        project['completed_tasks'] = []
                        
                    # Add to completed tasks
                    project['completed_tasks'].append(completed_task)
                    
                    # Store task results in long-term memory
                    self.memory_store.store_analysis_result(
                        project_id=self.project_id,
                        task_id=task_id,
                        task_type=completed_task['type'],
                        entities=completed_task['data'].get('entities', []),
                        results=completed_task['results']
                    )
                    
                elif status == 'failed' and results:
                    task['error'] = results
                
                break
        
        # If task wasn't found in active tasks and it's not a completion
        if not task_found and status != 'completed':
            new_task = {
                'task_id': task_id,
                'created_at': datetime.now().isoformat(),
                'status': status,
                'data': results if results else {},
                'session_id': self.session_id
            }
            if 'active_tasks' not in project:
                project['active_tasks'] = []
            project['active_tasks'].append(new_task)
        
        project['last_activity'] = datetime.now().isoformat()
        self.project_store.update_project(self.project_id, project)
    
    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """
        Get all active tasks in the current project.
        
        Returns:
            List of active tasks
        """
        project = self.project_store.get_project(self.project_id)
        return project.get('active_tasks', [])
    
    def get_completed_tasks(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get recently completed tasks, including those from long-term memory.
        
        Args:
            limit: Maximum number of tasks to return
            
        Returns:
            List of completed tasks
        """
        project = self.project_store.get_project(self.project_id)
        completed_tasks = project.get('completed_tasks', [])
        
        # Get tasks from the project store first
        recent_tasks = completed_tasks[-limit:] if completed_tasks else []
        
        # If we need more tasks, try to get historical tasks from memory store
        if len(recent_tasks) < limit:
            historical_results = self.memory_store.retrieve_similar_analyses(
                task_type="any",  # A placeholder value to get all tasks
                entities=[],
                limit=limit - len(recent_tasks)
            )
            
            # Convert memory_store results to task format and append
            for result in historical_results:
                if result.get('project_id') == self.project_id:
                    task = {
                        'task_id': result.get('task_id'),
                        'status': 'completed',
                        'task_type': result.get('task_type'),
                        'created_at': result.get('timestamp'),
                        'data': {
                            'task_type': result.get('task_type'),
                            'entities': result.get('entities', [])
                        },
                        'results': result.get('results', {}),
                        'from_long_term_memory': True
                    }
                    recent_tasks.append(task)
        
        return recent_tasks
    
    # Insight methods
    
    def add_insight(self, insight: Dict[str, Any]) -> None:
        """
        Add a generated insight to the project.
        
        Args:
            insight: The insight data including content and metadata
        """
        project = self.project_store.get_project(self.project_id)
        
        # Ensure insights list exists
        if 'insights' not in project:
            project['insights'] = []
            
        insight_with_metadata = insight.copy()
        insight_with_metadata['timestamp'] = datetime.now().isoformat()
        insight_with_metadata['session_id'] = self.session_id  # Track which session created this
        
        project['insights'].append(insight_with_metadata)
        project['last_activity'] = datetime.now().isoformat()
        self.project_store.update_project(self.project_id, project)
        
        # Store in long-term memory
        self.memory_store.store_insight(
            project_id=self.project_id,
            session_id=self.session_id,
            content=insight['content'],
            entities=insight.get('entities', []),
            context=self._get_recent_context(3)
        )
    
    def get_insights(self) -> List[Dict[str, Any]]:
        """
        Get all insights generated in the current project.
            
        Returns:
            List of insights
        """
        project = self.project_store.get_project(self.project_id)
        return project.get('insights', [])
    
    async def get_relevant_insights(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get insights relevant to the query from both project and memory store.
        
        Args:
            query: The query text to match against insights
            limit: Maximum number of insights to return
            
        Returns:
            List of relevant insights
        """
        return self.memory_store.retrieve_relevant_insights(
            query=query,
            limit=limit,
            user_id=self.user_id,
            project_id=self.project_id
        )
    
    # Code and variable tracking methods
    
    def track_code_version(self, code_id: str, version: str, changes: Dict[str, Any]) -> None:
        """
        Track code versions and changes over time.
        
        Args:
            code_id: Identifier for the code block
            version: Version identifier
            changes: What changed in this version
        """
        project = self.project_store.get_project(self.project_id)
        
        # Ensure code dict exists
        if 'code' not in project:
            project['code'] = {}
            
        if code_id not in project['code']:
            project['code'][code_id] = {
                'versions': []
            }
            
        # Add new version
        project['code'][code_id]['versions'].append({
            'version': version,
            'changes': changes,
            'timestamp': datetime.now().isoformat(),
            'session_id': self.session_id
        })
        
        project['last_activity'] = datetime.now().isoformat()
        self.project_store.update_project(self.project_id, project)
    
    def track_variable_state(self, variable_name: str, value: Any, context: str) -> None:
        """
        Track important variables and their changes.
        
        Args:
            variable_name: Name of the variable
            value: Current value of the variable
            context: Context in which the variable was set
        """
        project = self.project_store.get_project(self.project_id)
        
        # Ensure variables dict exists
        if 'variables' not in project:
            project['variables'] = {}
            
        if variable_name not in project['variables']:
            project['variables'][variable_name] = {
                'history': []
            }
            
        # Add new state
        project['variables'][variable_name]['history'].append({
            'value': value,
            'context': context,
            'timestamp': datetime.now().isoformat(),
            'session_id': self.session_id
        })
        
        # Update current value
        project['variables'][variable_name]['current_value'] = value
        
        project['last_activity'] = datetime.now().isoformat()
        self.project_store.update_project(self.project_id, project)
    
    def retrieve_data_asset_history(self, asset_id: str) -> List[Dict[str, Any]]:
        """
        Get the history of operations on a data asset.
        
        Args:
            asset_id: Identifier for the asset
            
        Returns:
            List of operations performed on the asset
        """
        # This would typically query both project store and memory store
        # For now, just return file info if it exists
        project = self.project_store.get_project(self.project_id)
        
        if 'files' in project and asset_id in project['files']:
            return [
                {
                    'operation': 'add_file',
                    'timestamp': project['files'][asset_id].get('added_at'),
                    'metadata': project['files'][asset_id].get('metadata', {})
                }
            ]
            
        return []
    
    # Context and utility methods
    
    def get_conversation_context(self) -> Dict[str, Any]:
        """
        Get the complete context needed for responding to the user.
        
        Returns:
            Dict containing conversation history, files, tasks, and insights
        """
        try:
            project = self.project_store.get_project(self.project_id)
            
            # Get session-specific messages if we have a session
            messages = []
            if self.session_id:
                try:
                    session = self.project_store.get_session(self.project_id, self.session_id)
                    messages = session.get('messages', [])
                except KeyError:
                    pass
            
            # Get relevant insights from long-term memory based on recent context
            recent_context = self._get_recent_context(3)
            relevant_insights = self.memory_store.retrieve_relevant_insights(
                query=recent_context,
                limit=5,
                user_id=self.user_id,
                project_id=self.project_id
            )
            
            return {
                'project_id': self.project_id,
                'session_id': self.session_id,
                'messages': messages,
                'files': project.get('files', {}),
                'active_tasks': project.get('active_tasks', []),
                'completed_tasks': project.get('completed_tasks', [])[-5:],  # Only recent ones
                'insights': project.get('insights', []) + relevant_insights,
                'code': project.get('code', {}),
                'variables': project.get('variables', {}),
                'project_created_at': project.get('created_at'),
                'project_duration': self._calculate_project_duration(project)
            }
            
        except KeyError:
            # Return minimal context if project doesn't exist
            return {
                'project_id': self.project_id,
                'session_id': self.session_id,
                'messages': [],
                'files': {},
                'active_tasks': [],
                'completed_tasks': [],
                'insights': []
            }
    
    def _get_recent_context(self, message_count: int) -> str:
        """Get recent conversation messages as a single string for context"""
        return self._get_recent_messages_text(message_count)
    
    def _get_recent_messages_text(self, count: int) -> str:
        """Extract text from recent messages"""
        messages = self.get_conversation_history(self.session_id, count)
        return "\n".join([f"{m['role']}: {m['content']}" for m in messages])
    
    def _calculate_project_duration(self, project: Dict[str, Any]) -> str:
        """Calculate the duration of the project so far"""
        if 'created_at' not in project:
            return "Unknown"
            
        start = datetime.fromisoformat(project['created_at'])
        last = datetime.fromisoformat(project['last_activity'])
        duration = last - start
        return str(duration)
    
    # Session and project lifecycle methods
    
    async def store_interaction(self, user_id: str, project_id: str, session_id: str, 
                               interaction_id: str, message: str, response: str, 
                               intent: str, entities: List[Any], is_followup: bool) -> None:
        """
        Store a complete user-assistant interaction.
        
        Args:
            user_id: User identifier
            project_id: Project identifier
            session_id: Session identifier
            interaction_id: Unique identifier for this interaction
            message: The user's message
            response: The assistant's response
            intent: Detected intent
            entities: Extracted entities
            is_followup: Whether this is a followup question
        """
        # Update our internal state to match parameters
        self.user_id = user_id
        self.project_id = project_id
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
    
    def clear_session(self, session_id: Optional[str] = None) -> None:
        """Clear a session while keeping the project"""
        target_session = session_id or self.session_id
        if target_session:
            self.project_store.delete_session(self.project_id, target_session)
            if target_session == self.session_id:
                self.session_id = None
    
    def clear_project(self) -> None:
        """Clear the entire project"""
        self.project_store.delete_project(self.project_id)
    
    async def project_exists(self, project_id: str) -> bool:
        """Check if a project exists"""
        return self.project_store.project_exists(project_id)
    
    async def session_exists(self, project_id: str, session_id: str) -> bool:
        """Check if a session exists within a project"""
        return self.project_store.session_exists(project_id, session_id)
    
    # New methods
    
    def add_task_to_project(self, user_id: str, project_id: str, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Add a new task to the project's memory (called from TaskCreator).
        
        Args:
            user_id: User identifier
            project_id: Project identifier
            task_id: Task identifier
            task_data: Complete task data
        """
        # Update our internal state if not already set
        if self.project_id != project_id:
            self.project_id = project_id
            self.user_id = user_id
            
            # Ensure project exists
            if not self.project_store.project_exists(project_id):
                self.initialize_project()
        
        # Add task to project using standard add_task method
        self.add_task(task_id, task_data)

    def update_task_in_project(self, project_id: str, task_id: str, status: str, results: Optional[Dict[str, Any]] = None) -> None:
        """
        Update task status in project memory (called from QueueManager).
        
        Args:
            project_id: Project identifier
            task_id: Task identifier
            status: New status value
            results: Optional task results
        """
        # Update our internal state if needed
        if self.project_id != project_id:
            self.project_id = project_id
        
        # Update task using standard update method
        self.update_task_status(task_id, status, results)

    def add_insight_to_project(self, project_id: str, insight_data: Dict[str, Any]) -> None:
        """
        Add an insight directly from a task to project memory (called from QueueManager).
        
        Args:
            project_id: Project identifier
            insight_data: The insight data to add
        """
        # Update our internal state if needed
        if self.project_id != project_id:
            self.project_id = project_id
        
        # Add insight using standard add_insight method
        self.add_insight(insight_data)
        
    async def get_recent_history(self, user_id: str, project_id: str, session_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent conversation history.
        Wrapper for get_conversation_history to match the expected interface.
        
        Args:
            user_id: User identifier (not used in this implementation)
            project_id: Project identifier (not used if already set in constructor)
            session_id: Specific session ID
            limit: Maximum number of messages to return
            
        Returns:
            List of recent messages
        """
        # If the project_id is different, update it
        if project_id != self.project_id:
            self.project_id = project_id
        
        return self.get_conversation_history(session_id, limit)
        