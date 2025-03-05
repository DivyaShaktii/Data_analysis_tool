from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query
from typing import List, Dict, Any, Optional
import uuid
import os
import logging
from services.file_parser import FileParser
import numpy as np
import pandas as pd


def convert_numpy_types(obj: Any) -> Any:
    """Convert numpy types to Python native types recursively"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        # Handle NaN and infinite values
        if np.isnan(obj):
            return None
        if np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, np.ndarray):
        # Handle NaN values in arrays
        arr = obj.tolist()
        return [None if isinstance(x, float) and np.isnan(x) else x for x in arr]
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif pd.isna(obj):  # Handle pandas NA/NaN values if present
        return None
    return obj

router = APIRouter(prefix="/data", tags=["data"])
logger = logging.getLogger(__name__)
file_parser = FileParser()

# Store active sessions (in memory for now, would use proper DB in production)
active_sessions = {}

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    session_id: Optional[str] = Query(None)
):
    """
    Upload a data file for analysis.
    If session_id is not provided, a new session will be created.
    """
    if not session_id:
        session_id = str(uuid.uuid4())
        active_sessions[session_id] = {"files": []}
    
    elif session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    try:
        # Create a session directory if it doesn't exist
        os.makedirs(f"./data/uploads/{session_id}", exist_ok=True)
        
        # Save the file
        file_path = f"./data/uploads/{session_id}/{file.filename}"
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Parse file to get initial metadata
        metadata = await file_parser.parse_file_metadata(file_path)
        
        # Convert numpy types to Python native types
        metadata = convert_numpy_types(metadata)
        
        # Update session information
        file_info = {
            "filename": file.filename,
            "path": file_path,
            "metadata": metadata
        }
        active_sessions[session_id]["files"].append(file_info)
        
        return {
            "session_id": session_id,
            "filename": file.filename,
            "metadata": metadata
        }
        
    except Exception as e:
        logger.error(f"Error processing file upload: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@router.get("/sessions/{session_id}")
async def get_session_info(session_id: str):
    """Get information about a specific analysis session"""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    return active_sessions[session_id]

@router.get("/preview/{session_id}/{filename}")
async def preview_data(
    session_id: str,
    filename: str,
    rows: int = Query(10, ge=1, le=100)
):
    """Get a preview of the data"""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Find the file in the session
    file_info = None
    for file in active_sessions[session_id]["files"]:
        if file["filename"] == filename:
            file_info = file
            break
    
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found in session")
    
    try:
        preview_data = await file_parser.get_preview(file_info["path"], rows)
        # Convert numpy types to Python native types
        preview_data = convert_numpy_types(preview_data)
        return preview_data
    except Exception as e:
        logger.error(f"Error generating preview: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating preview: {str(e)}")