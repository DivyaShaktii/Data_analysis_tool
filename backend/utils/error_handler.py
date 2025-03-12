# backend/utils/error_handler.py
import traceback
from typing import Dict, Any, Optional, List, Type
from fastapi import HTTPException
from utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

class BaseAppError(Exception):
    """Base error class for application-specific exceptions"""
    
    def __init__(
        self, 
        message: str, 
        status_code: int = 500, 
        details: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.error_code = error_code or "INTERNAL_ERROR"
        super().__init__(self.message)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert the error to a dictionary for API responses"""
        return {
            "error": self.error_code,
            "message": self.message,
            "status_code": self.status_code,
            "details": self.details
        }
        
    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException"""
        return HTTPException(
            status_code=self.status_code,
            detail=self.to_dict()
        )

# Specific error classes
class ValidationError(BaseAppError):
    """Error raised when input validation fails"""
    
    def __init__(
        self, 
        message: str, 
        details: Optional[Dict[str, Any]] = None,
        error_code: str = "VALIDATION_ERROR"
    ):
        super().__init__(
            message=message,
            status_code=422,
            details=details,
            error_code=error_code
        )

class NotFoundError(BaseAppError):
    """Error raised when a requested resource is not found"""
    
    def __init__(
        self, 
        message: str, 
        resource_type: str,
        resource_id: str,
        error_code: str = "NOT_FOUND"
    ):
        details = {
            "resource_type": resource_type,
            "resource_id": resource_id
        }
        super().__init__(
            message=message,
            status_code=404,
            details=details,
            error_code=error_code
        )

class AuthenticationError(BaseAppError):
    """Error raised when authentication fails"""
    
    def __init__(
        self, 
        message: str = "Authentication failed",
        details: Optional[Dict[str, Any]] = None,
        error_code: str = "AUTHENTICATION_ERROR"
    ):
        super().__init__(
            message=message,
            status_code=401,
            details=details,
            error_code=error_code
        )

class AuthorizationError(BaseAppError):
    """Error raised when a user lacks permission for an operation"""
    
    def __init__(
        self, 
        message: str = "Not authorized to perform this operation",
        details: Optional[Dict[str, Any]] = None,
        error_code: str = "AUTHORIZATION_ERROR"
    ):
        super().__init__(
            message=message,
            status_code=403,
            details=details,
            error_code=error_code
        )

class TaskError(BaseAppError):
    """Error raised when a task operation fails"""
    
    def __init__(
        self, 
        message: str,
        task_id: Optional[str] = None,
        error_code: str = "TASK_ERROR"
    ):
        details = {"task_id": task_id} if task_id else {}
        super().__init__(
            message=message,
            status_code=500,
            details=details,
            error_code=error_code
        )

class DataProcessingError(BaseAppError):
    """Error raised during data processing operations"""
    
    def __init__(
        self, 
        message: str,
        file_name: Optional[str] = None,
        operation: Optional[str] = None,
        error_code: str = "DATA_PROCESSING_ERROR"
    ):
        details = {}
        if file_name:
            details["file_name"] = file_name
        if operation:
            details["operation"] = operation
            
        super().__init__(
            message=message,
            status_code=500,
            details=details,
            error_code=error_code
        )

class LLMError(BaseAppError):
    """Error raised when interacting with LLM services"""
    
    def __init__(
        self, 
        message: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        error_code: str = "LLM_ERROR"
    ):
        details = {}
        if provider:
            details["provider"] = provider
        if model:
            details["model"] = model
            
        super().__init__(
            message=message,
            status_code=502,  # Bad Gateway for external service failures
            details=details,
            error_code=error_code
        )

# Error handling functions
def log_exception(
    exception: Exception, 
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log an exception with additional context
    
    Args:
        exception (Exception): The exception to log
        context (dict, optional): Additional context about when the error occurred
    """
    error_details = {
        "error_type": type(exception).__name__,
        "error_message": str(exception),
        "traceback": traceback.format_exc()
    }
    
    if context:
        error_details["context"] = context
        
    logger.error(
        f"Exception: {type(exception).__name__}: {str(exception)}",
        extra={"error_details": error_details}
    )

def handle_exception(
    exception: Exception, 
    context: Optional[Dict[str, Any]] = None
) -> HTTPException:
    """
    Handle and convert an exception to an HTTPException
    
    Args:
        exception (Exception): The exception to handle
        context (dict, optional): Additional context about when the error occurred
        
    Returns:
        HTTPException: A FastAPI HTTPException with appropriate status code and details
    """
    # Log the exception
    log_exception(exception, context)
    
    # Handle our custom exceptions
    if isinstance(exception, BaseAppError):
        return exception.to_http_exception()
    
    # Handle FastAPI HTTPExceptions
    if isinstance(exception, HTTPException):
        return exception
        
    # Default error handling for unknown exceptions
    return HTTPException(
        status_code=500,
        detail={
            "error": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
            "details": {"error_type": type(exception).__name__}
        }
    )