"""
metadata_extractor.py
Extracts basic metadata from data files without analyzing content.
"""

import os
import hashlib
from typing import Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MetadataExtractor:
    """
    Extracts essential metadata from data files.
    Focuses on file properties without analyzing content.
    """
    
    def __init__(self):
        """Initialize the MetadataExtractor."""
        pass
    
    def extract_basic_metadata(self, file_path: str, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract basic metadata from a file without analyzing content.
        
        Args:
            file_path: Path to the file
            file_info: Basic file information from FileHandler
            
        Returns:
            Dictionary containing essential metadata
        """
        if not os.path.exists(file_path):
            return {"error": "File not found"}
        
        # Start with the file info we already have
        metadata = file_info.copy()
        
        # Add file system metadata
        metadata.update(self._get_file_system_metadata(file_path))
        
        # Add file fingerprint (hash)
        metadata["fingerprint"] = self._generate_file_hash(file_path)
        
        return metadata
    
    def _get_file_system_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get basic file system metadata.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file system metadata
        """
        file_stats = os.stat(file_path)
        
        return {
            "file_size_bytes": file_stats.st_size,
            "file_size_mb": round(file_stats.st_size / (1024 * 1024), 2),
            "last_modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
            "created": datetime.fromtimestamp(file_stats.st_ctime).isoformat(),
            "file_extension": file_path.split(".")[-1].lower(),
            "file_name": os.path.basename(file_path)
        }
    
    def _generate_file_hash(self, file_path: str) -> str:
        """
        Generate a hash for a file based on content sampling.
        
        Args:
            file_path: Path to the file
            
        Returns:
            String representation of the file hash
        """
        # Get file modification time and size
        mod_time = os.path.getmtime(file_path)
        file_size = os.path.getsize(file_path)
        
        # Sample file content (first 8KB)
        with open(file_path, 'rb') as f:
            content_sample = f.read(8192)
        
        # Create hash from these components
        hasher = hashlib.md5()
        hasher.update(str(mod_time).encode('utf-8'))
        hasher.update(str(file_size).encode('utf-8'))
        hasher.update(content_sample)
        
        return hasher.hexdigest()