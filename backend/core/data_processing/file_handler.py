"""
file_handler.py
Handles file uploads, storage, and retrieval for the Agentic Analytics System.
Validates files and manages their storage without performing analytical processing.
"""

import os
import uuid
import pandas as pd
from typing import Dict, List, Optional, Tuple, BinaryIO, Union
import logging
from pathlib import Path
import shutil
import mimetypes

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'xls': 'application/vnd.ms-excel',
    'json': 'application/json',
    'parquet': 'application/octet-stream',
    'txt': 'text/plain'
}

class FileHandler:
    """
    Handles file operations for the Agentic Analytics System.
    Responsible for validating, uploading, storing, and retrieving user files.
    """
    
    def __init__(self, storage_path: str = "./data/uploads/"):
        """
        Initialize the FileHandler with a storage path.
        
        Args:
            storage_path: Directory where files will be stored
        """
        self.storage_path = storage_path
        self._ensure_storage_exists()
    
    def _ensure_storage_exists(self) -> None:
        """Create storage directory if it doesn't exist."""
        os.makedirs(self.storage_path, exist_ok=True)
        logger.info(f"Storage directory set to {self.storage_path}")
    
    def _generate_file_id(self) -> str:
        """Generate a unique file ID."""
        return str(uuid.uuid4())
    
    def _get_file_path(self, file_id: str, extension: str) -> str:
        """Get the full file path for a given file ID and extension."""
        return os.path.join(self.storage_path, f"{file_id}.{extension}")
    
    def is_valid_file(self, filename: str) -> bool:
        """
        Check if a file has a supported extension.
        
        Args:
            filename: Name of the file to check
            
        Returns:
            True if file extension is supported, False otherwise
        """
        if not filename or '.' not in filename:
            return False
        
        extension = filename.split(".")[-1].lower()
        return extension in SUPPORTED_EXTENSIONS
    
    def validate_file_size(self, file_size: int, max_size_mb: int = 50) -> bool:
        """
        Check if a file size is within the allowed limit.
        
        Args:
            file_size: Size of the file in bytes
            max_size_mb: Maximum allowed size in megabytes
            
        Returns:
            True if file size is within limit, False otherwise
        """
        max_size_bytes = max_size_mb * 1024 * 1024
        return file_size <= max_size_bytes
    
    def save_file(self, file_content: BinaryIO, filename: str, user_id: str) -> Dict:
        """
        Save an uploaded file to storage.
        
        Args:
            file_content: The file content as bytes or file-like object
            filename: Original filename
            user_id: ID of the user who uploaded the file
            
        Returns:
            Dict containing file metadata
        """
        # Extract extension and validate
        if not self.is_valid_file(filename):
            raise ValueError(f"Unsupported file type. Supported types: {', '.join(SUPPORTED_EXTENSIONS.keys())}")
        
        extension = filename.split(".")[-1].lower()
        
        # Generate a unique ID for the file
        file_id = self._generate_file_id()
        file_path = self._get_file_path(file_id, extension)
        
        # Save the file
        with open(file_path, "wb") as f:
            if hasattr(file_content, 'read'):
                shutil.copyfileobj(file_content, f)
            else:
                f.write(file_content)
        
        # Get file size and validate
        file_size = os.path.getsize(file_path)
        if not self.validate_file_size(file_size):
            os.remove(file_path)  # Remove file if it exceeds size limit
            raise ValueError(f"File size exceeds the maximum allowed limit")
        
        # Create metadata
        file_metadata = {
            "file_id": file_id,
            "filename": filename,
            "extension": extension,
            "mime_type": SUPPORTED_EXTENSIONS[extension],
            "user_id": user_id,
            "upload_timestamp": pd.Timestamp.now().isoformat(),
            "file_path": file_path,
            "size_bytes": file_size
        }
        
        logger.info(f"File saved: {filename} (ID: {file_id}) for user {user_id}")
        return file_metadata
    
    def get_file(self, file_id: str, extension: Optional[str] = None) -> Tuple[str, str]:
        """
        Retrieve a file by its ID.
        
        Args:
            file_id: Unique identifier for the file
            extension: File extension (optional if known)
            
        Returns:
            Tuple of (file_path, extension)
        """
        if extension:
            file_path = self._get_file_path(file_id, extension)
            if os.path.exists(file_path):
                return file_path, extension
        else:
            # If extension not provided, look for any file with the ID
            for ext in SUPPORTED_EXTENSIONS:
                file_path = self._get_file_path(file_id, ext)
                if os.path.exists(file_path):
                    return file_path, ext
        
        raise FileNotFoundError(f"File with ID {file_id} not found")
    
    def delete_file(self, file_id: str, extension: Optional[str] = None) -> bool:
        """
        Delete a file from storage.
        
        Args:
            file_id: Unique identifier for the file
            extension: File extension (optional if known)
            
        Returns:
            True if file was deleted, False if file was not found
        """
        try:
            file_path, ext = self.get_file(file_id, extension)
            os.remove(file_path)
            logger.info(f"Deleted file: {file_id}.{ext}")
            return True
        except FileNotFoundError:
            logger.warning(f"Attempted to delete non-existent file: {file_id}")
            return False
    
    def list_user_files(self, user_id: str) -> List[Dict]:
        """
        List all files uploaded by a specific user.
        
        Args:
            user_id: ID of the user
            
        Returns:
            List of file metadata dictionaries
        """
        # In a real implementation, this would query a database
        # This is a simplified version that scans the directory
        user_files = []
        for file in os.listdir(self.storage_path):
            file_path = os.path.join(self.storage_path, file)
            # Simple implementation - in production, would query DB by user_id
            if os.path.isfile(file_path):
                try:
                    file_id, extension = file.rsplit(".", 1)
                    # Create minimal metadata for listing
                    file_metadata = {
                        "file_id": file_id,
                        "filename": file,
                        "extension": extension,
                        "size_bytes": os.path.getsize(file_path),
                        "last_modified": pd.Timestamp(os.path.getmtime(file_path), unit='s').isoformat()
                    }
                    user_files.append(file_metadata)
                except Exception as e:
                    logger.error(f"Error accessing file {file}: {str(e)}")
        
        return user_files
    
    def check_file_readability(self, file_id: str, extension: Optional[str] = None) -> Dict:
        """
        Check if a file can be read as a valid data file.
        
        Args:
            file_id: Unique identifier for the file
            extension: File extension (optional if known)
            
        Returns:
            Dict with readability status and basic info
        """
        try:
            file_path, ext = self.get_file(file_id, extension)
            result = {"readable": False, "format": ext}
            
            # Just check if pandas can read the file
            if ext == 'csv':
                pd.read_csv(file_path, nrows=5)
                result["readable"] = True
            elif ext in ['xlsx', 'xls']:
                pd.read_excel(file_path, nrows=5)
                result["readable"] = True
            elif ext == 'json':
                pd.read_json(file_path)
                result["readable"] = True
            elif ext == 'parquet':
                pd.read_parquet(file_path)
                result["readable"] = True
            elif ext == 'txt':
                # Try to read as CSV with various delimiters
                try:
                    pd.read_csv(file_path, sep=None, engine='python', nrows=5)
                    result["readable"] = True
                except:
                    # At least confirm we can read the file
                    with open(file_path, 'r') as f:
                        f.read(100)  # Read first 100 chars
                    result["readable"] = True
                    result["format_note"] = "Plain text file - may need special processing"
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking file readability: {str(e)}")
            return {
                "readable": False,
                "error": str(e)
            }