"""
data_inspector.py
Performs basic inspection of data files to extract schema and structural information.
"""

import pandas as pd
import json
import os
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class DataInspector:
    """
    Responsible for basic inspection of data files to extract schema and structure.
    Prepares metadata for the analytics agents without performing in-depth analysis.
    """
    
    def __init__(self):
        """Initialize the DataInspector."""
        pass
    
    def inspect_file(self, file_path: str, sample_rows: int = 5) -> Dict[str, Any]:
        """
        Perform basic inspection of a data file.
        
        Args:
            file_path: Path to the file to inspect
            sample_rows: Number of rows to sample for inspection
            
        Returns:
            Dictionary containing basic file structure information
        """
        if not os.path.exists(file_path):
            return {"error": "File not found"}
        
        file_extension = file_path.split(".")[-1].lower()
        
        try:
            # Basic file info
            file_info = {
                "file_size_bytes": os.path.getsize(file_path),
                "file_extension": file_extension,
                "last_modified": pd.Timestamp(os.path.getmtime(file_path), unit='s').isoformat()
            }
            
            # Read file based on type
            if file_extension == 'csv':
                df = pd.read_csv(file_path, nrows=sample_rows)
                file_info.update(self._get_tabular_info(df, file_path, 'csv'))
                
            elif file_extension in ['xlsx', 'xls']:
                # Get sheet names first
                sheet_names = pd.ExcelFile(file_path).sheet_names
                file_info["sheet_names"] = sheet_names
                file_info["sheet_count"] = len(sheet_names)
                
                # Read first sheet for preview
                df = pd.read_excel(file_path, sheet_name=sheet_names[0], nrows=sample_rows)
                file_info.update(self._get_tabular_info(df, file_path, 'excel', sheet=sheet_names[0]))
                
            elif file_extension == 'json':
                # Try to determine if it's records or a nested structure
                with open(file_path, 'r') as f:
                    try:
                        # Just read start of file to check structure
                        start_content = f.read(1000)
                        first_char = start_content.strip()[0] if start_content.strip() else None
                        
                        if first_char == '[':  # Likely array of records
                            df = pd.read_json(file_path)
                            file_info.update(self._get_tabular_info(df, file_path, 'json'))
                        else:  # Nested structure or single object
                            json_data = json.loads(start_content + f.read())  # Read the rest
                            file_info.update({
                                "format": "json",
                                "structure": "nested" if isinstance(json_data, dict) else "unknown",
                                "top_level_keys": list(json_data.keys()) if isinstance(json_data, dict) else None
                            })
                    except json.JSONDecodeError:
                        # Partial read might not be valid JSON, so retry with full file
                        f.seek(0)
                        try:
                            json_data = json.load(f)
                            file_info.update({
                                "format": "json",
                                "structure": "nested" if isinstance(json_data, dict) else "array",
                                "top_level_keys": list(json_data.keys()) if isinstance(json_data, dict) else None
                            })
                        except json.JSONDecodeError as e:
                            file_info.update({
                                "format": "json",
                                "error": f"Invalid JSON: {str(e)}"
                            })
                
            elif file_extension == 'parquet':
                df = pd.read_parquet(file_path)
                file_info.update(self._get_tabular_info(df, file_path, 'parquet'))
                
            elif file_extension == 'txt':
                # Try to detect delimiter
                try:
                    df = pd.read_csv(file_path, sep=None, engine='python', nrows=sample_rows)
                    file_info.update(self._get_tabular_info(df, file_path, 'delimited text'))
                except:
                    # If it can't be read as delimited, treat as plain text
                    with open(file_path, 'r') as f:
                        sample_content = f.read(1000)
                    
                    file_info.update({
                        "format": "plain text",
                        "sample_content": sample_content[:200] + ("..." if len(sample_content) > 200 else ""),
                        "line_count_estimate": sum(1 for _ in open(file_path, 'r', encoding='utf-8', errors='ignore'))
                    })
            
            else:
                file_info.update({
                    "format": "unknown",
                    "note": f"Unsupported file extension: {file_extension}"
                })
            
            return file_info
            
        except Exception as e:
            logger.error(f"Error inspecting file {file_path}: {str(e)}")
            return {
                "error": str(e),
                "file_extension": file_extension,
                "file_size_bytes": os.path.getsize(file_path)
            }
    
    def _get_tabular_info(self, df: pd.DataFrame, file_path: str, format_type: str, 
                          sheet: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract basic information from a tabular dataset.
        
        Args:
            df: DataFrame containing the data
            file_path: Path to the source file
            format_type: Type of the file format (csv, excel, etc.)
            sheet: Sheet name for Excel files
            
        Returns:
            Dictionary with basic tabular information
        """
        if df.empty:
            return {
                "format": format_type,
                "sheet": sheet,
                "is_empty": True
            }
        
        # Get column info
        columns_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            # Simplify pandas dtype names for better readability
            if 'int' in dtype:
                simple_type = 'integer'
            elif 'float' in dtype:
                simple_type = 'float'
            elif 'bool' in dtype:
                simple_type = 'boolean'
            elif 'datetime' in dtype:
                simple_type = 'datetime'
            else:
                simple_type = 'string/object'
                
            columns_info.append({
                "name": str(col),
                "type": simple_type,
                "nullable": df[col].isna().any()
            })
        
        # Count rows in the full file without loading it all in memory
        row_count = None
        try:
            if format_type == 'csv':
                # Count lines and subtract header
                row_count = sum(1 for _ in open(file_path, 'r')) - 1
            elif format_type == 'excel' and sheet:
                # This gets row count but loads the whole sheet
                # In production, use a more efficient method for large files
                xl = pd.ExcelFile(file_path)
                row_count = len(pd.read_excel(xl, sheet_name=sheet))
        except Exception as e:
            logger.warning(f"Could not count rows: {str(e)}")
            row_count = "unknown"
        
        # Sample data (first few rows)
        sample_records = df.head(5).to_dict(orient='records')
        
        return {
            "format": format_type,
            "sheet": sheet,
            "column_count": len(df.columns),
            "row_count": row_count,
            "columns": columns_info,
            "sample_records": sample_records
        }