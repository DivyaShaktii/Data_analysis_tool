# backend/core/memory/session_store.py

"""
Session Store for maintaining medium-term memory of user sessions.
This module provides functionality to store and retrieve session information
that persists throughout a user's interaction with the system.
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)

class SessionStore:
    """
    Manages session data for active users, providing methods to create,
    retrieve, update, and delete sessions.
    
    This implementation uses in-memory storage with file backup.
    For production, consider using Redis, MongoDB, or another appropriate database.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SessionStore, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        """Initialize the session store as a singleton"""
        if self._initialized:
            return
            
        self._sessions = {}
        self._session_dir = os.environ.get('SESSION_DIR', 'data/sessions')
        self._session_ttl = int(os.environ.get('SESSION_TTL_HOURS', 24))
        
        # Create session directory if it doesn't exist
        os.makedirs(self._session_dir, exist_ok=True)
        
        # Load existing sessions from disk
        self._load_sessions()
        
        # Start periodic cleanup of expired sessions
        self._start_cleanup_thread()
        
        self._initialized = True
    
    def create_session(self, session_id: str, initial_data: Dict[str, Any] = None) -> None:
        """
        Create a new session with the given ID.
        
        Args:
            session_id: Unique identifier for the session
            initial_data: Optional initial data for the session
        """
        if initial_data is None:
            initial_data = {}
        
        # Add metadata if not already present
        if 'session_start' not in initial_data:
            initial_data['session_start'] = datetime.now().isoformat()
        if 'last_activity' not in initial_data:
            initial_data['last_activity'] = datetime.now().isoformat()
        
        self._sessions[session_id] = initial_data
        self._save_session(session_id)
        logger.info(f"Created new session: {session_id}")
    
    def get_session(self, session_id: str) -> Dict[str, Any]:
        """
        Retrieve a session by ID.
        
        Args:
            session_id: Unique identifier for the session
            
        Returns:
            Session data dictionary
            
        Raises:
            KeyError: If the session doesn't exist
        """
        if not self.session_exists(session_id):
            logger.warning(f"Attempted to access non-existent session: {session_id}")
            raise KeyError(f"Session {session_id} not found")
        
        # Update last activity time
        self._sessions[session_id]['last_activity'] = datetime.now().isoformat()
        return self._sessions[session_id]
    
    def update_session(self, session_id: str, data: Dict[str, Any]) -> None:
        """
        Update an existing session with new data.
        
        Args:
            session_id: Unique identifier for the session
            data: New session data
            
        Raises:
            KeyError: If the session doesn't exist
        """
        if not self.session_exists(session_id):
            logger.warning(f"Attempted to update non-existent session: {session_id}")
            raise KeyError(f"Session {session_id} not found")
        
        self._sessions[session_id] = data
        self._save_session(session_id)
    
    def delete_session(self, session_id: str) -> None:
        """
        Delete a session.
        
        Args:
            session_id: Unique identifier for the session
        """
        if self.session_exists(session_id):
            del self._sessions[session_id]
            
            # Remove session file if it exists
            session_path = os.path.join(self._session_dir, f"{session_id}.json")
            if os.path.exists(session_path):
                os.remove(session_path)
                logger.info(f"Deleted session file: {session_id}")
    
    def session_exists(self, session_id: str) -> bool:
        """
        Check if a session exists.
        
        Args:
            session_id: Unique identifier for the session
            
        Returns:
            True if the session exists, False otherwise
        """
        return session_id in self._sessions
    
    def get_all_sessions(self) -> List[str]:
        """
        Get a list of all active session IDs.
        
        Returns:
            List of session IDs
        """
        return list(self._sessions.keys())
    
    def get_active_sessions(self, hours: int = 1) -> List[str]:
        """
        Get sessions that have been active within the specified time period.
        
        Args:
            hours: Number of hours to consider for activity
            
        Returns:
            List of active session IDs
        """
        active_time = datetime.now() - timedelta(hours=hours)
        active_sessions = []
        
        for session_id, data in self._sessions.items():
            last_activity = datetime.fromisoformat(data['last_activity'])
            if last_activity >= active_time:
                active_sessions.append(session_id)
        
        return active_sessions
    
    def _save_session(self, session_id: str) -> None:
        """Save a session to disk"""
        session_path = os.path.join(self._session_dir, f"{session_id}.json")
        try:
            with open(session_path, 'w') as f:
                json.dump(self._sessions[session_id], f)
        except Exception as e:
            logger.error(f"Error saving session {session_id}: {str(e)}")
    
    def _load_sessions(self) -> None:
        """Load all sessions from disk"""
        try:
            for filename in os.listdir(self._session_dir):
                if filename.endswith('.json'):
                    session_id = filename[:-5]  # Remove the .json extension
                    session_path = os.path.join(self._session_dir, filename)
                    
                    try:
                        with open(session_path, 'r') as f:
                            session_data = json.load(f)
                            self._sessions[session_id] = session_data
                    except Exception as e:
                        logger.error(f"Error loading session {session_id}: {str(e)}")
            
            logger.info(f"Loaded {len(self._sessions)} sessions from disk")
        except Exception as e:
            logger.error(f"Error loading sessions directory: {str(e)}")
    
    def _cleanup_expired_sessions(self) -> None:
        """Remove sessions that have been inactive beyond the TTL"""
        expiration_time = datetime.now() - timedelta(hours=self._session_ttl)
        expired_sessions = []
        
        for session_id, data in self._sessions.items():
            try:
                last_activity = datetime.fromisoformat(data['last_activity'])
                if last_activity < expiration_time:
                    expired_sessions.append(session_id)
            except (KeyError, ValueError) as e:
                logger.error(f"Error checking session expiration for {session_id}: {str(e)}")
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            self.delete_session(session_id)
        
        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
    
    def _start_cleanup_thread(self) -> None:
        """Start a background thread for periodic session cleanup"""
        def cleanup_job():
            self._cleanup_expired_sessions()
            
            # Schedule the next cleanup in 1 hour
            threading.Timer(3600, cleanup_job).start()
        
        # Start the first cleanup job
        cleanup_thread = threading.Thread(target=cleanup_job)
        cleanup_thread.daemon = True
        cleanup_thread.start()