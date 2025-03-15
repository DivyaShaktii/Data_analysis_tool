# backend/api/routers/data_router.py
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request
from typing import Optional, List, Dict, Any
import pandas as pd
import io
import json

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
        
        # Extract metadata
        metadata_extractor = MetadataExtractor()
        metadata = metadata_extractor.extract_metadata(file_path, file_metadata)
        
        # Inspect data
        data_inspector = DataInspector()
        data_info = data_inspector.inspect_file(file_path)
        
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
                "data_info": data_info
            }
        )
        
        return {
            "message": f"File {file.filename} uploaded successfully",
            "session_id": session_id,
            "file_info": {
                "filename": file.filename,
                "metadata": metadata,
                "data_info": data_info
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
        context_manager = req.state.context_manager
        file_context = context_manager.get_file_context()
        
        if not file_context:
            raise HTTPException(status_code=404, detail="No file data found for this session")
        
        # Get the first file in the context (or you could add a file_id parameter to the endpoint)
        file_id = next(iter(file_context))
        file_info = file_context[file_id]['metadata']
        
        file_handler = FileHandler()
        preview_data = file_handler.get_data_preview(file_info["file_path"], rows)
        
        return {
            "filename": file_info["filename"],
            "preview": preview_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data preview: {str(e)}")