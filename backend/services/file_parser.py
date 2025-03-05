import pandas as pd
import numpy as np
import json
import os
from typing import Dict, Any, List, Optional, Tuple
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class FileParser:
    """Service for parsing and extracting information from data files"""
    
    supported_extensions = {
        ".csv": "csv",
        ".xlsx": "excel",
        ".xls": "excel",
        ".parquet": "parquet",
        ".json": "json",
        ".txt": "text"
    }
    
    def __init__(self):
        pass
    
    async def parse_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract basic metadata from a file without loading the entire dataset
        """
        try:
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext not in self.supported_extensions:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            file_type = self.supported_extensions[file_ext]
            
            # Process based on file type
            if file_type == "csv":
                return await self._process_csv_metadata(file_path)
            elif file_type == "excel":
                return await self._process_excel_metadata(file_path)
            elif file_type == "parquet":
                return await self._process_parquet_metadata(file_path)
            elif file_type == "json":
                return await self._process_json_metadata(file_path)
            elif file_type == "text":
                return await self._process_text_metadata(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
                
        except Exception as e:
            logger.error(f"Error parsing file metadata: {str(e)}")
            raise
    
    async def get_preview(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Get a preview of the data with specified number of rows
        """
        try:
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext not in self.supported_extensions:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            file_type = self.supported_extensions[file_ext]
            
            # Process based on file type
            if file_type == "csv":
                df = pd.read_csv(file_path, nrows=rows)
            elif file_type == "excel":
                df = pd.read_excel(file_path, nrows=rows)
            elif file_type == "parquet":
                df = pd.read_parquet(file_path)
                df = df.head(rows)
            elif file_type == "json":
                df = pd.read_json(file_path)
                df = df.head(rows)
            elif file_type == "text":
                # Simple text preview
                with open(file_path, 'r') as f:
                    lines = [line.strip() for line in f.readlines()[:rows]]
                return {
                    "type": "text",
                    "lines": lines,
                    "row_count": len(lines)
                }
            
            # For DataFrame-based results
            return {
                "type": "tabular",
                "columns": df.columns.tolist(),
                "data": df.replace({np.nan: None}).to_dict('records'),
                "row_count": len(df)
            }
                
        except Exception as e:
            logger.error(f"Error generating preview: {str(e)}")
            raise
    
    async def _process_csv_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from CSV files"""
        # Read just a few rows to infer schema
        df_sample = pd.read_csv(file_path, nrows=100)
        
        # Count total rows (more efficient than loading entire file)
        row_count = sum(1 for _ in open(file_path, 'r'))
        # Subtract 1 for header
        row_count -= 1
        
        return {
            "file_type": "csv",
            "row_count": row_count,
            "column_count": len(df_sample.columns),
            "columns": [
                {
                    "name": col,
                    "dtype": str(df_sample[col].dtype),
                    "sample": df_sample[col].iloc[0] if not df_sample.empty else None
                }
                for col in df_sample.columns
            ],
            "memory_usage": df_sample.memory_usage(deep=True).sum()
        }
    
    async def _process_excel_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from Excel files"""
        # Get sheet names
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        
        # Read first sheet for sample
        df_sample = pd.read_excel(file_path, sheet_name=sheet_names[0], nrows=100)
        
        return {
            "file_type": "excel",
            "sheets": sheet_names,
            "active_sheet": sheet_names[0],
            "row_count": len(df_sample),  # This is just for the sample
            "column_count": len(df_sample.columns),
            "columns": [
                {
                    "name": col,
                    "dtype": str(df_sample[col].dtype),
                    "sample": df_sample[col].iloc[0] if not df_sample.empty else None
                }
                for col in df_sample.columns
            ],
            "memory_usage": df_sample.memory_usage(deep=True).sum()
        }
    
    async def _process_parquet_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from Parquet files"""
        # Parquet files have built-in metadata
        df_sample = pd.read_parquet(file_path)
        
        return {
            "file_type": "parquet",
            "row_count": len(df_sample),
            "column_count": len(df_sample.columns),
            "columns": [
                {
                    "name": col,
                    "dtype": str(df_sample[col].dtype),
                    "sample": df_sample[col].iloc[0] if not df_sample.empty else None
                }
                for col in df_sample.columns
            ],
            "memory_usage": df_sample.memory_usage(deep=True).sum()
        }
    
    
        
    async def _process_json_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from JSON files"""
        # Try to infer if it's a records format or something else
        with open(file_path, 'r') as f:
            # Just read beginning to check structure
            start = f.read(1024)
            
            # Check if it starts with [ for records format
            is_records = start.strip().startswith('[')
        
        if is_records:
            df_sample = pd.read_json(file_path)
            
            return {
                "file_type": "json",
                "format": "records",
                "row_count": len(df_sample),
                "column_count": len(df_sample.columns),
                "columns": [
                    {
                        "name": col,
                        "dtype": str(df_sample[col].dtype),
                        "sample": df_sample[col].iloc[0] if not df_sample.empty else None
                    }
                    for col in df_sample.columns
                ],
                "memory_usage": df_sample.memory_usage(deep=True).sum()
            }
        else:
            # For nested JSON or non-tabular format
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                    
                    # Try to get some basic info about the structure
                    if isinstance(data, dict):
                        top_level_keys = list(data.keys())
                        return {
                            "file_type": "json",
                            "format": "nested",
                            "top_level_keys": top_level_keys,
                            "structure_sample": str(data)[:200] + "..." if len(str(data)) > 200 else str(data),
                            "approx_size": os.path.getsize(file_path)
                        }
                    else:
                        return {
                            "file_type": "json",
                            "format": "unknown",
                            "structure_sample": str(data)[:200] + "..." if len(str(data)) > 200 else str(data),
                            "approx_size": os.path.getsize(file_path)
                        }
                except json.JSONDecodeError:
                    raise ValueError("Invalid JSON format")
                
    async def _process_text_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from text files"""
        # Just get the first 100 lines
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f.readlines()[:100]]
        return {
            "file_type": "text",
            "row_count": len(lines),
            "sample": lines[:10]
        }       
