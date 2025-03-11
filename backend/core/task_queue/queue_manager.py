# backend/core/task_queue/queue_manager.py
from typing import Dict, List, Optional, Any, Callable, Awaitable
import logging
import asyncio
from datetime import datetime
import heapq
from collections import deque

from .task_schema import Task, TaskStatus

logger = logging.getLogger(__name__)


class TaskQueueManager:
    """
    Manages the queue of analytical tasks, handling prioritization,
    dependencies, and dispatching tasks to the execution system.
    """
    
    def __init__(self, storage_connector=None, max_concurrent_tasks: int = 5):
        """
        Initialize the task queue manager.
        
        Args:
            storage_connector: Connection to persistent storage for tasks
            max_concurrent_tasks: Maximum number of tasks that can run concurrently
        """
        self.tasks: Dict[str, Task] = {}  # All tasks (by ID)
        self.task_queue = []  # Priority queue for pending tasks
        self.running_tasks: Dict[str, Task] = {}  # Currently running tasks
        self.completed_tasks: deque = deque(maxlen=100)  # Recently completed tasks
        
        self.storage = storage_connector
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_handlers: Dict[str, Callable[[Task], Awaitable[Dict[str, Any]]]] = {}
        
        self._queue_lock = asyncio.Lock()
        logger.info("Task Queue Manager initialized")
    
    async def enqueue(self, task: Task) -> str:
        """
        Add a task to the queue with appropriate priority.
        
        Args:
            task: The task to be queued
            
        Returns:
            The task ID
        """
        async with self._queue_lock:
            # Store the task
            self.tasks[task.id] = task
            
            # Add to priority queue if it's ready to run
            if not task.dependencies or self._all_dependencies_met(task):
                self._add_to_priority_queue(task)
            
            # Persist the task if storage is available
            if self.storage:
                await self.storage.save_task(task.to_dict())
            
            logger.info(f"Task {task.id} of type {task.task_type} enqueued with priority {task.priority}")
            return task.id
    
    def register_handler(self, task_type: str, handler: Callable[[Task], Awaitable[Dict[str, Any]]]) -> None:
        """
        Register a handler function for a specific task type.
        
        Args:
            task_type: The type of task this handler processes
            handler: Async function that processes the task and returns results
        """
        self.task_handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")
    
    async def start_processing(self) -> None:
        """Start the background task processing loop"""
        logger.info("Starting task processing loop")
        while True:
            await self._process_next_tasks()
            await asyncio.sleep(1)  # Prevent tight loop
    
    async def get_task(self, task_id: str) -> Optional[Task]:
        """
        Get a task by ID.
        
        Args:
            task_id: The task identifier
            
        Returns:
            The task if found, None otherwise
        """
        # Try in-memory first
        if task_id in self.tasks:
            return self.tasks[task_id]
        
        # Try from storage if available
        if self.storage:
            task_data = await self.storage.get_task(task_id)
            if task_data:
                task = Task.from_dict(task_data)
                self.tasks[task_id] = task
                return task
        
        return None
    
    async def get_tasks_by_session(self, session_id: str) -> List[Task]:
        """
        Get all tasks for a specific session.
        
        Args:
            session_id: The session identifier
            
        Returns:
            List of tasks for the session
        """
        # Filter in-memory tasks first
        session_tasks = [task for task in self.tasks.values() if task.session_id == session_id]
        
        # Get from storage if available
        if self.storage:
            stored_tasks = await self.storage.get_tasks_by_session(session_id)
            # Add any missing tasks to our in-memory store
            for task_data in stored_tasks:
                task = Task.from_dict(task_data)
                if task.id not in self.tasks:
                    self.tasks[task.id] = task
                    session_tasks.append(task)
        
        return session_tasks
    
    async def get_active_tasks(self, session_id: Optional[str] = None) -> List[Task]:
        """
        Get tasks that are currently queued or in progress.
        
        Args:
            session_id: Optional session to filter by
            
        Returns:
            List of active tasks
        """
        active_tasks = [
            task for task in self.tasks.values() 
            if task.status in (TaskStatus.QUEUED, TaskStatus.IN_PROGRESS)
            and (session_id is None or task.session_id == session_id)
        ]
        return active_tasks
    
    async def get_completed_tasks(self, session_id: Optional[str] = None, limit: int = 10) -> List[Task]:
        """
        Get recently completed tasks.
        
        Args:
            session_id: Optional session to filter by
            limit: Maximum number of tasks to return
            
        Returns:
            List of completed tasks
        """
        # Start with in-memory completed tasks
        completed = list(self.completed_tasks)
        
        # Filter by session if needed
        if session_id:
            completed = [task for task in completed if task.session_id == session_id]
        
        # Get from storage if available and needed
        if self.storage and len(completed) < limit:
            stored_completed = await self.storage.get_completed_tasks(
                session_id=session_id, 
                limit=limit - len(completed)
            )
            for task_data in stored_completed:
                task = Task.from_dict(task_data)
                if task not in completed:
                    completed.append(task)
        
        # Sort by completion time (newest first) and limit
        completed.sort(key=lambda t: t.completed_at or "", reverse=True)
        return completed[:limit]
    
    async def update_task_status(self, task_id: str, status: TaskStatus, 
                                 results: Optional[Dict[str, Any]] = None,
                                 error: Optional[str] = None) -> Optional[Task]:
        """
        Update a task's status and optionally add results or error info.
        
        Args:
            task_id: The task identifier
            status: New status for the task
            results: Optional results to store (for completed tasks)
            error: Optional error message (for failed tasks)
            
        Returns:
            The updated task or None if not found
        """
        task = await self.get_task(task_id)
        if not task:
            logger.warning(f"Cannot update status for unknown task {task_id}")
            return None
            
        # Update the task
        task.update_status(status, error)
        
        if results and status == TaskStatus.COMPLETED:
            task.add_results(results)
        
        # Update dependent tasks if this one is now complete
        if status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
            
            self.completed_tasks.appendleft(task)
            await self._check_dependent_tasks(task_id)
        
        # Persist changes if storage is available
        if self.storage:
            await self.storage.update_task(task.to_dict())
        
        logger.info(f"Updated task {task_id} status to {status}")
        return task
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a queued task.
        
        Args:
            task_id: The task identifier
            
        Returns:
            True if successful, False otherwise
        """
        task = await self.get_task(task_id)
        if not task:
            logger.warning(f"Cannot cancel unknown task {task_id}")
            return False
            
        if task.status != TaskStatus.QUEUED:
            logger.warning(f"Cannot cancel task {task_id} with status {task.status}")
            return False
        
        # Update task status
        await self.update_task_status(task_id, TaskStatus.CANCELLED)
        
        # Rebuild priority queue without this task
        async with self._queue_lock:
            self._rebuild_priority_queue()
        
        logger.info(f"Cancelled task {task_id}")
        return True
    
    async def _process_next_tasks(self) -> None:
        """Process the next available tasks in the queue"""
        async with self._queue_lock:
            # Check if we can start any new tasks
            available_slots = self.max_concurrent_tasks - len(self.running_tasks)
            if available_slots <= 0 or not self.task_queue:
                return
            
            # Start as many tasks as we can
            tasks_to_start = []
            while available_slots > 0 and self.task_queue:
                # Get highest priority task
                _, _, task_id = heapq.heappop(self.task_queue)
                
                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    if task.status == TaskStatus.QUEUED:
                        tasks_to_start.append(task)
                        available_slots -= 1
            
        # Execute tasks outside the lock
        for task in tasks_to_start:
            asyncio.create_task(self._execute_task(task))
    
    async def _execute_task(self, task: Task) -> None:
        """
        Execute a single task by calling the appropriate handler.
        
        Args:
            task: Task to execute
        """
        logger.info(f"Starting execution of task {task.id} of type {task.task_type}")
        
        # Mark as in progress and add to running tasks
        await self.update_task_status(task.id, TaskStatus.IN_PROGRESS)
        self.running_tasks[task.id] = task
        
        # Find the appropriate handler
        handler = self.task_handlers.get(task.task_type)
        if not handler:
            # Try to find a default handler
            handler = self.task_handlers.get("default")
            
        if not handler:
            error_msg = f"No handler registered for task type: {task.task_type}"
            logger.error(error_msg)
            await self.update_task_status(
                task.id, 
                TaskStatus.FAILED, 
                error=error_msg
            )
            return
            
        # Execute the handler
        try:
            results = await handler(task)
            await self.update_task_status(
                task.id, 
                TaskStatus.COMPLETED, 
                results=results
            )
            logger.info(f"Successfully completed task {task.id}")
            
        except Exception as e:
            error_msg = f"Error executing task {task.id}: {str(e)}"
            logger.exception(error_msg)
            await self.update_task_status(
                task.id, 
                TaskStatus.FAILED, 
                error=error_msg
            )
    
    async def _check_dependent_tasks(self, completed_task_id: str) -> None:
        """
        Check if any pending tasks depend on the completed task and
        enqueue them if all their dependencies are met.
        
        Args:
            completed_task_id: ID of the task that was just completed
        """
        async with self._queue_lock:
            # Find tasks that depend on this one
            dependent_tasks = [
                task for task in self.tasks.values()
                if (task.status == TaskStatus.QUEUED and
                    completed_task_id in task.dependencies)
            ]
            
            # Check if they're ready to run
            for task in dependent_tasks:
                if self._all_dependencies_met(task):
                    self._add_to_priority_queue(task)
                    logger.info(f"Dependency met for task {task.id}, adding to queue")
    
    def _all_dependencies_met(self, task: Task) -> bool:
        """
        Check if all dependencies for a task have been completed.
        
        Args:
            task: The task to check
            
        Returns:
            True if all dependencies are met, False otherwise
        """
        for dep_id in task.dependencies:
            dep_task = self.tasks.get(dep_id)
            if not dep_task or dep_task.status != TaskStatus.COMPLETED:
                return False
        return True
    
    def _add_to_priority_queue(self, task: Task) -> None:
        """
        Add a task to the priority queue.
        
        Args:
            task: Task to add to the queue
        """
        # Our heap is a min-heap, so we negate priority to get highest priority first
        # We also use insertion time as a secondary sort key
        entry = (-task.priority, datetime.now().timestamp(), task.id)
        heapq.heappush(self.task_queue, entry)
    
    def _rebuild_priority_queue(self) -> None:
        """Rebuild the priority queue, removing cancelled or completed tasks"""
        new_queue = []
        for priority, timestamp, task_id in self.task_queue:
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.QUEUED:
                heapq.heappush(new_queue, (priority, timestamp, task_id))
        
        self.task_queue = new_queue