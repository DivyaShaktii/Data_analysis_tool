"""
Project Store for maintaining medium-term memory of user projects.
This module provides functionality to store and retrieve project information
that persists throughout a user's interaction with the system.
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)

class ProjectStore:
    """
    Manages project data for active users, providing methods to create,
    retrieve, update, and delete projects.
    
    This implementation uses in-memory storage with file backup.
    For production, consider using Redis, MongoDB, or another appropriate database.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ProjectStore, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        """Initialize the project store as a singleton"""
        if self._initialized:
            return
            
        self._projects = {}
        self._project_dir = os.environ.get('PROJECT_DIR', 'data/projects')
        self._project_ttl = int(os.environ.get('PROJECT_TTL_DAYS', 90))
        
        # Create project directory if it doesn't exist
        os.makedirs(self._project_dir, exist_ok=True)
        
        # Load existing projects from disk
        self._load_projects()
        
        # Start periodic cleanup of expired projects
        self._start_cleanup_thread()
        
        self._initialized = True
    
    def create_project(self, project_id: str, user_id: str, initial_data: Dict[str, Any] = None) -> None:
        """
        Create a new project with the given ID.
        
        Args:
            project_id: Unique identifier for the project
            user_id: ID of the user who owns this project
            initial_data: Optional initial data for the project
        """
        if initial_data is None:
            initial_data = {}
        
        # Ensure project has the necessary structure
        project_data = {
            'user_id': user_id,
            'project_id': project_id,
            'sessions': {},
            'files': {},
            'active_tasks': [],
            'completed_tasks': [],
            'insights': [],
            'code': {},
            'variables': {},
            'created_at': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat()
        }
        
        # Merge with initial data if provided
        project_data.update(initial_data)
        
        self._projects[project_id] = project_data
        self._save_project(project_id)
        logger.info(f"Created new project: {project_id} for user {user_id}")
        
    def create_session(self, project_id: str, session_id: str, session_data: Dict[str, Any] = None) -> None:
        """
        Create a new session within a project.
        
        Args:
            project_id: Project this session belongs to
            session_id: Unique identifier for the session
            session_data: Optional initial session data
        """
        if not self.project_exists(project_id):
            raise KeyError(f"Project {project_id} not found")
            
        if session_data is None:
            session_data = {}
            
        # Create session with basic structure
        session = {
            'session_id': session_id,
            'messages': [],
            'session_start': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat()
        }
        
        # Merge with provided data
        session.update(session_data)
        
        # Add to project
        project = self._projects[project_id]
        project['sessions'][session_id] = session
        project['last_activity'] = datetime.now().isoformat()
        
        self._save_project(project_id)
        logger.info(f"Created new session {session_id} in project {project_id}")
    
    def get_project(self, project_id: str) -> Dict[str, Any]:
        """
        Retrieve a project by ID.
        
        Args:
            project_id: Unique identifier for the project
            
        Returns:
            Project data dictionary
            
        Raises:
            KeyError: If the project doesn't exist
        """
        if not self.project_exists(project_id):
            logger.warning(f"Attempted to access non-existent project: {project_id}")
            raise KeyError(f"Project {project_id} not found")
        
        # Update last activity time
        self._projects[project_id]['last_activity'] = datetime.now().isoformat()
        return self._projects[project_id]
    
    def get_session(self, project_id: str, session_id: str) -> Dict[str, Any]:
        """
        Retrieve a session within a project.
        
        Args:
            project_id: Project identifier
            session_id: Session identifier
            
        Returns:
            Session data dictionary
            
        Raises:
            KeyError: If project or session doesn't exist
        """
        project = self.get_project(project_id)
        
        if session_id not in project['sessions']:
            logger.warning(f"Attempted to access non-existent session: {session_id} in project {project_id}")
            raise KeyError(f"Session {session_id} not found in project {project_id}")
            
        # Update session and project activity time
        project['sessions'][session_id]['last_activity'] = datetime.now().isoformat()
        project['last_activity'] = datetime.now().isoformat()
        
        return project['sessions'][session_id]
    
    def update_project(self, project_id: str, data: Dict[str, Any]) -> None:
        """
        Update an existing project with new data.
        
        Args:
            project_id: Unique identifier for the project
            data: New project data
            
        Raises:
            KeyError: If the project doesn't exist
        """
        if not self.project_exists(project_id):
            logger.warning(f"Attempted to update non-existent project: {project_id}")
            raise KeyError(f"Project {project_id} not found")
        
        self._projects[project_id] = data
        self._save_project(project_id)
    
    def update_session(self, project_id: str, session_id: str, data: Dict[str, Any]) -> None:
        """
        Update an existing session with new data.
        
        Args:
            project_id: Project identifier
            session_id: Session identifier
            data: New session data
            
        Raises:
            KeyError: If project or session doesn't exist
        """
        project = self.get_project(project_id)
        
        if session_id not in project['sessions']:
            raise KeyError(f"Session {session_id} not found in project {project_id}")
            
        project['sessions'][session_id] = data
        project['last_activity'] = datetime.now().isoformat()
        
        self._save_project(project_id)
    
    def delete_project(self, project_id: str) -> None:
        """
        Delete a project.
        
        Args:
            project_id: Unique identifier for the project
        """
        if self.project_exists(project_id):
            del self._projects[project_id]
            
            # Remove project file if it exists
            project_path = os.path.join(self._project_dir, f"{project_id}.json")
            if os.path.exists(project_path):
                os.remove(project_path)
                logger.info(f"Deleted project file: {project_id}")
    
    def delete_session(self, project_id: str, session_id: str) -> None:
        """
        Delete a session from a project.
        
        Args:
            project_id: Project identifier
            session_id: Session identifier to delete
        """
        if self.project_exists(project_id):
            project = self._projects[project_id]
            if session_id in project['sessions']:
                del project['sessions'][session_id]
                project['last_activity'] = datetime.now().isoformat()
                self._save_project(project_id)
                logger.info(f"Deleted session {session_id} from project {project_id}")
    
    def project_exists(self, project_id: str) -> bool:
        """
        Check if a project exists.
        
        Args:
            project_id: Unique identifier for the project
            
        Returns:
            True if the project exists, False otherwise
        """
        return project_id in self._projects
    
    def session_exists(self, project_id: str, session_id: str) -> bool:
        """
        Check if a session exists within a project.
        
        Args:
            project_id: Project identifier
            session_id: Session identifier
            
        Returns:
            True if the session exists, False otherwise
        """
        if not self.project_exists(project_id):
            return False
        return session_id in self._projects[project_id]['sessions']
    
    def get_user_projects(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get all projects belonging to a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of project summary dictionaries
        """
        user_projects = []
        
        for project_id, project in self._projects.items():
            if project.get('user_id') == user_id:
                # Create a summary (avoid returning full project data)
                user_projects.append({
                    'project_id': project_id,
                    'created_at': project.get('created_at'),
                    'last_activity': project.get('last_activity'),
                    'session_count': len(project.get('sessions', {})),
                    'file_count': len(project.get('files', {})),
                    'insight_count': len(project.get('insights', []))
                })
                
        return user_projects
    
    def get_active_projects(self, days: int = 30) -> List[str]:
        """
        Get projects that have been active within the specified time period.
        
        Args:
            days: Number of days to consider for activity
            
        Returns:
            List of active project IDs
        """
        active_time = datetime.now() - timedelta(days=days)
        active_projects = []
        
        for project_id, data in self._projects.items():
            last_activity = datetime.fromisoformat(data['last_activity'])
            if last_activity >= active_time:
                active_projects.append(project_id)
        
        return active_projects
    
    def _save_project(self, project_id: str) -> None:
        """Save a project to disk"""
        project_path = os.path.join(self._project_dir, f"{project_id}.json")
        try:
            with open(project_path, 'w') as f:
                json.dump(self._projects[project_id], f)
        except Exception as e:
            logger.error(f"Error saving project {project_id}: {str(e)}")
    
    def _load_projects(self) -> None:
        """Load all projects from disk"""
        try:
            for filename in os.listdir(self._project_dir):
                if filename.endswith('.json'):
                    project_id = filename[:-5]  # Remove the .json extension
                    project_path = os.path.join(self._project_dir, filename)
                    
                    try:
                        with open(project_path, 'r') as f:
                            project_data = json.load(f)
                            self._projects[project_id] = project_data
                    except Exception as e:
                        logger.error(f"Error loading project {project_id}: {str(e)}")
            
            logger.info(f"Loaded {len(self._projects)} projects from disk")
        except Exception as e:
            logger.error(f"Error loading projects directory: {str(e)}")
    
    def _cleanup_expired_projects(self) -> None:
        """Remove projects that have been inactive beyond the TTL"""
        expiration_time = datetime.now() - timedelta(days=self._project_ttl)
        expired_projects = []
        
        for project_id, data in self._projects.items():
            try:
                last_activity = datetime.fromisoformat(data['last_activity'])
                if last_activity < expiration_time:
                    expired_projects.append(project_id)
            except (KeyError, ValueError) as e:
                logger.error(f"Error checking project expiration for {project_id}: {str(e)}")
                expired_projects.append(project_id)
        
        for project_id in expired_projects:
            self.delete_project(project_id)
        
        if expired_projects:
            logger.info(f"Cleaned up {len(expired_projects)} expired projects")
    
    def _start_cleanup_thread(self) -> None:
        """Start a background thread for periodic project cleanup"""
        def cleanup_job():
            self._cleanup_expired_projects()
            
            # Schedule the next cleanup in 24 hours
            threading.Timer(86400, cleanup_job).start()
        
        # Start the first cleanup job
        cleanup_thread = threading.Thread(target=cleanup_job)
        cleanup_thread.daemon = True
        cleanup_thread.start() 