# backend/utils/logger.py
import logging
import os
import sys
from datetime import datetime

def setup_logger(name, log_level=None):
    """
    Set up a logger with the specified name and log level.
    
    Args:
        name (str): The name of the logger (typically __name__ from the calling module)
        log_level (int, optional): The logging level. Defaults to the LOG_LEVEL env var or INFO.
    
    Returns:
        logging.Logger: A configured logger instance
    """
    # Get log level from environment variable or use default
    if log_level is None:
        log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create handlers if they don't exist
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Create formatter
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(console_handler)
        
        # File handler (optional)
        log_dir = os.environ.get("LOG_DIR", "logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        log_file = os.path.join(
            log_dir, 
            f"app_{datetime.now().strftime('%Y%m%d')}.log"
        )
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Configure root logger for third-party libraries
def configure_root_logger():
    """Configure the root logger for third-party libraries"""
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Set more restrictive level for third-party libraries
    logging.basicConfig(
        level=logging.WARNING,  # Default level for other loggers
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Only set our application loggers to the specified level
    for logger_name in logging.root.manager.loggerDict:
        if logger_name.startswith("agentic_analytics"):
            logging.getLogger(logger_name).setLevel(log_level)

# Call this function when the application starts
configure_root_logger()

# Default logger
default_logger = setup_logger("agentic_analytics")