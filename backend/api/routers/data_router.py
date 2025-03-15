# backend/api/routers/data_router.py
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request
from typing import Optional, List, Dict, Any
import pandas as pd
import io
import json
import os

from core.data_processing.file_handler import FileHandler
from core.data_processing.data_inspector import DataInspector
from core.data_processing.metadata_extractor import MetadataExtractor

router = APIRouter()

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    req: Request = None,
):
    try:
        # Read file content
        contents = await file.read()
        file_handler = FileHandler()
        
        # Save file and get file metadata
        file_metadata = file_handler.save_file(contents, file.filename, session_id)
        
        # Extract file path from metadata
        file_path = file_metadata["file_path"]
        
        # Extract basic metadata without reading the data
        metadata_extractor = MetadataExtractor()
        metadata = metadata_extractor.extract_basic_metadata(file_path, file_metadata)
        
        # Validate file format without analyzing content
        data_inspector = DataInspector()
        format_info = data_inspector.validate_file_format(file_path)
        
        # Add file info to context
        context_manager = req.state.context_manager
        
        # Generate a file_id (you can use the one from file_metadata if available)
        file_id = file_metadata.get("file_id", f"{session_id}_{file.filename}")
        
        # Use the correct method name and parameters
        context_manager.add_file(
            file_id=file_id,
            metadata={
                "filename": file.filename,
                "file_path": file_path,
                "metadata": metadata,
                "format_info": format_info
            }
        )
        
        return {
            "message": f"File {file.filename} uploaded successfully",
            "session_id": session_id,
            "file_info": {
                "filename": file.filename,
                "metadata": metadata,
                "format_info": format_info
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")

@router.get("/info/{session_id}")
async def get_data_info(
    session_id: str,
    req: Request
):
    try:
        context_manager = req.state.context_manager
        file_info = context_manager.get_file_context()
        if not file_info:
            raise HTTPException(status_code=404, detail="No file data found for this session")
        return file_info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving file info: {str(e)}")

@router.get("/preview/{session_id}")
async def get_data_preview(
    session_id: str,
    req: Request,
    rows: int = 10,
):
    try:
        # Get file context from context manager
        context_manager = req.state.context_manager
        file_context = context_manager.get_file_context()
        
        if not file_context:
            raise HTTPException(status_code=404, detail="No file data found for this session")
        
        # Get the first file in the context (or you could add a file_id parameter to the endpoint)
        file_id = next(iter(file_context))
        file_info = file_context[file_id]['metadata']
        
        # Get file path from context
        file_path = file_info.get("file_path")
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on server")
        
        # Get file extension
        file_extension = file_path.split(".")[-1].lower()
        
        # Create a safe preview that doesn't try to read the data
        # Just return the metadata and format info we already have
        preview_data = {
            "filename": file_info["filename"],
            "metadata": file_info["metadata"],
            "format_info": file_info.get("format_info", {}),
            "preview_note": "Data preview is disabled to avoid serialization issues. Use the chat interface to ask questions about the data."
        }
        
        # Add file type specific information
        if file_extension in ['csv', 'xlsx', 'xls', 'parquet']:
            preview_data["data_type"] = "tabular"
        elif file_extension == 'json':
            preview_data["data_type"] = "structured"
        elif file_extension == 'txt':
            preview_data["data_type"] = "text"
        else:
            preview_data["data_type"] = "unknown"
        
        return preview_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data preview: {str(e)}")

@router.get("/files/{session_id}")
async def list_session_files(
    session_id: str,
    req: Request
):
    try:
        context_manager = req.state.context_manager
        file_context = context_manager.get_file_context()
        
        if not file_context:
            return {"files": []}
        
        # Extract basic file info for listing
        files = []
        for file_id, file_data in file_context.items():
            metadata = file_data.get('metadata', {})
            files.append({
                "file_id": file_id,
                "filename": metadata.get("filename", "Unknown"),
                "file_size": metadata.get("metadata", {}).get("file_size_mb", 0),
                "format": metadata.get("format_info", {}).get("format", "unknown"),
                "upload_timestamp": metadata.get("metadata", {}).get("upload_timestamp", "")
            })
        
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")

@router.delete("/files/{session_id}/{file_id}")
async def delete_file(
    session_id: str,
    file_id: str,
    req: Request
):
    try:
        # Get file context
        context_manager = req.state.context_manager
        file_context = context_manager.get_file_context()
        
        if not file_context or file_id not in file_context:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get file path
        file_path = file_context[file_id]['metadata'].get('file_path')
        
        # Delete file from storage
        file_handler = FileHandler()
        if file_path and os.path.exists(file_path):
            # Get extension from path
            extension = file_path.split(".")[-1].lower()
            # Delete the file
            deleted = file_handler.delete_file(file_id, extension)
            if not deleted:
                raise HTTPException(status_code=500, detail="Failed to delete file from storage")
        
        # Remove from context
        context_manager.remove_file(file_id)
        
        return {"message": "File deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")