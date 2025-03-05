import logging
import pandas as pd
import numpy as np
import json
import uuid
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import os
from datetime import datetime

from Data_analysis_tool.backend.agents.coder_agent import CodeWriterAgent

logger = logging.getLogger(__name__)

class AnalysisService:
    """Service for analyzing data and generating insights"""
    
    def __init__(self):
        self.code_writer = CodeWriterAgent()
        
    async def analyze_data(self, 
                          file_path: str, 
                          question: str, 
                          context: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Analyze data based on user question and context
        """
        try:
            # Identify file type
            file_ext = Path(file_path).suffix.lower()
            
            # Get data
            df = self._load_dataframe(file_path)
            
            # Generate and execute analysis code
            code, result, charts = await self.code_writer.generate_analysis(
                df=df,
                question=question,
                context=context,
                file_type=file_ext
            )
            
            # Format response
            return {
                "code": code,
                "result": result,
                "visualizations": charts,
                "metadata": {
                    "execution_time": datetime.now().isoformat(),
                    "file_path": file_path,
                    "question": question
                }
            }
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            raise
    
    async def get_data_profile(self, file_path: str) -> Dict[str, Any]:
        """
        Generate a profile of the dataset with basic statistics
        """
        try:
            # Load data
            df = self._load_dataframe(file_path)
            
            # Basic dataset info
            profile = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "memory_usage": df.memory_usage(deep=True).sum(),
                "columns": []
            }
            
            # Get column profiles
            for column in df.columns:
                col_profile = self._get_column_profile(df, column)
                profile["columns"].append(col_profile)
            
            # Generate summary statistics
            profile["summary"] = {
                "missing_values": df.isna().sum().sum(),
                "missing_percentage": (df.isna().sum().sum() / (df.shape[0] * df.shape[1])) * 100,
                "duplicate_rows": df.duplicated().sum(),
                "duplicate_percentage": (df.duplicated().sum() / len(df)) * 100 if len(df) > 0 else 0
            }
            
            return profile
            
        except Exception as e:
            logger.error(f"Error generating data profile: {str(e)}")
            raise
    
    def _get_column_profile(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """Generate profile for a single column"""
        col_data = df[column]
        dtype = col_data.dtype
        
        profile = {
            "name": column,
            "dtype": str(dtype),
            "missing_count": col_data.isna().sum(),
            "missing_percentage": (col_data.isna().sum() / len(df)) * 100 if len(df) > 0 else 0,
            "unique_count": col_data.nunique()
        }
        
        # Add type-specific statistics
        if pd.api.types.is_numeric_dtype(dtype):
            profile.update({
                "min": float(col_data.min()) if not col_data.empty and not np.all(col_data.isna()) else None,
                "max": float(col_data.max()) if not col_data.empty and not np.all(col_data.isna()) else None,
                "mean": float(col_data.mean()) if not col_data.empty and not np.all(col_data.isna()) else None,
                "median": float(col_data.median()) if not col_data.empty and not np.all(col_data.isna()) else None,
                "std": float(col_data.std()) if not col_data.empty and not np.all(col_data.isna()) else None,
            })
        elif pd.api.types.is_string_dtype(dtype):
            # For string columns
            non_na_values = col_data.dropna()
            if not non_na_values.empty:
                profile.update({
                    "min_length": min(non_na_values.str.len()),
                    "max_length": max(non_na_values.str.len()),
                    "avg_length": non_na_values.str.len().mean(),
                    "sample_values": non_na_values.sample(min(5, len(non_na_values))).tolist()
                })
        elif pd.api.types.is_datetime64_dtype(dtype):
            # For datetime columns
            non_na_values = col_data.dropna()
            if not non_na_values.empty:
                profile.update({
                    "min_date": non_na_values.min().isoformat(),
                    "max_date": non_na_values.max().isoformat(),
                    "date_range_days": (non_na_values.max() - non_na_values.min()).days
                })
        
        return profile
    
    def _load_dataframe(self, file_path: str) -> pd.DataFrame:
        """Load data file into pandas DataFrame based on file extension"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            return pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        elif file_ext == '.parquet':
            return pd.read_parquet(file_path)
        elif file_ext == '.json':
            # Try to determine if it's records format
            with open(file_path, 'r') as f:
                start = f.read(1024)
                is_records = start.strip().startswith('[')
                
            if is_records:
                return pd.read_json(file_path)
            else:
                # For nested JSON, normalize if possible
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if isinstance(data, dict):
                    # Try to flatten if it's a nested dict
                    try:
                        return pd.json_normalize(data)
                    except:
                        # If normalization fails, return as is
                        return pd.DataFrame([data])
                else:
                    # Return as is if not a dict
                    return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
    
    async def suggest_analyses(self, file_path: str) -> List[Dict[str, str]]:
        """
        Suggest potential analyses based on data profile
        """
        try:
            # Get data profile
            profile = await self.get_data_profile(file_path)
            
            # Load data
            df = self._load_dataframe(file_path)
            
            # Generate suggestions based on data types
            suggestions = []
            
            # Check for numeric columns for statistical analysis
            numeric_cols = [col["name"] for col in profile["columns"] 
                          if "numeric" in col["dtype"] or "float" in col["dtype"] or "int" in col["dtype"]]
            
            if len(numeric_cols) >= 1:
                suggestions.append({
                    "title": "Descriptive Statistics",
                    "description": f"Get basic statistics for numeric columns: {', '.join(numeric_cols[:3])}...",
                    "question": "Show me descriptive statistics for the numeric columns"
                })
            
            if len(numeric_cols) >= 2:
                suggestions.append({
                    "title": "Correlation Analysis",
                    "description": "Find relationships between numeric variables",
                    "question": "Calculate and visualize correlations between numeric columns"
                })
            
            # Check for datetime columns for time series analysis
            datetime_cols = [col["name"] for col in profile["columns"] if "datetime" in col["dtype"]]
            
            if len(datetime_cols) >= 1 and len(numeric_cols) >= 1:
                suggestions.append({
                    "title": "Time Series Analysis",
                    "description": f"Analyze trends over time using {datetime_cols[0]}",
                    "question": f"Show me trends over time for {numeric_cols[0]} by {datetime_cols[0]}"
                })
            
            # Check for high cardinality string columns for categorical analysis
            categorical_cols = [col["name"] for col in profile["columns"] 
                              if "object" in col["dtype"] and col.get("unique_count", 0) < 20]
            
            if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
                suggestions.append({
                    "title": "Group Analysis",
                    "description": f"Compare {numeric_cols[0]} across different {categorical_cols[0]} groups",
                    "question": f"Compare {numeric_cols[0]} grouped by {categorical_cols[0]}"
                })
            
            # Data quality suggestion if there are missing values
            if profile["summary"]["missing_values"] > 0:
                suggestions.append({
                    "title": "Data Quality Assessment",
                    "description": "Analyze missing values and data quality issues",
                    "question": "Show me a summary of missing values and data quality issues"
                })
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error generating analysis suggestions: {str(e)}")
            raise

    async def process_message(
        self,
        session_id: str,
        message: str,
        conversation_history: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process a chat message and generate a response
        
        Args:
            session_id: The current session ID
            message: The user's message
            conversation_history: List of previous messages in the conversation
            
        Returns:
            Dict containing response text and optional code/insights
        """
        try:
            # Initialize response
            response_data = {
                "response": "",
                "code": None,
                "insights": None
            }
            
            # Get the active files for this session from the conversation context
            # This would need to be implemented based on your data storage approach
            active_files = self._get_active_files(session_id)
            
            if not active_files:
                response_data["response"] = "Please upload a data file first to analyze."
                return response_data
            
            # Analyze the latest uploaded file
            latest_file = active_files[-1]
            
            # Generate analysis based on the message
            analysis_result = await self.analyze_data(
                file_path=latest_file["path"],
                question=message,
                context=conversation_history
            )
            
            # Format the response
            response_data.update({
                "response": analysis_result.get("result", "Analysis completed."),
                "code": analysis_result.get("code"),
                "insights": analysis_result.get("visualizations")
            })
            
            return response_data
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            raise
    
    def _get_active_files(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Get active files for a session from the data router's storage
        """
        # Import here to avoid circular imports
        from routers.data import active_sessions
        
        if session_id in active_sessions:
            return active_sessions[session_id].get("files", [])
        return []