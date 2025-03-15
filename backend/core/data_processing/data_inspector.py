"""
data_inspector.py
Performs basic inspection of data files to validate format without analyzing content.
"""

import os
from typing import Dict, Any
import logging
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

class DataInspector:
    """
    Responsible for basic inspection of data files to validate format.
    Does not analyze content to avoid serialization issues.
    """
    
    def __init__(self):
        """Initialize the DataInspector."""
        pass
    
    def validate_file_format(self, file_path: str) -> Dict[str, Any]:
        """
        Validate a file's format without reading its content.
        
        Args:
            file_path: Path to the file to inspect
            
        Returns:
            Dictionary containing basic file format information
        """
        if not os.path.exists(file_path):
            return {"error": "File not found"}
        
        file_extension = file_path.split(".")[-1].lower()
        
        try:
            # Basic file info
            file_info = {
                "file_size_bytes": os.path.getsize(file_path),
                "file_extension": file_extension,
                "last_modified": os.path.getmtime(file_path)
            }
            
            # Determine format based on extension
            if file_extension in SUPPORTED_EXTENSIONS:
                file_info.update({
                    "format": file_extension,
                    "mime_type": SUPPORTED_EXTENSIONS[file_extension],
                    "is_supported": True
                })
                
                # Add format-specific info without reading the file
                if file_extension == 'csv':
                    file_info.update({"format_type": "tabular"})
                elif file_extension in ['xlsx', 'xls']:
                    file_info.update({"format_type": "tabular"})
                elif file_extension == 'json':
                    file_info.update({"format_type": "structured"})
                elif file_extension == 'parquet':
                    file_info.update({"format_type": "tabular"})
                elif file_extension == 'txt':
                    file_info.update({"format_type": "text"})
            else:
                file_info.update({
                    "format": "unknown",
                    "is_supported": False,
                    "note": f"Unsupported file extension: {file_extension}"
                })
            
            return file_info
            
        except Exception as e:
            logger.error(f"Error validating file format {file_path}: {str(e)}")
            return {
                "error": str(e),
                "file_extension": file_extension,
                "file_size_bytes": os.path.getsize(file_path)
            }