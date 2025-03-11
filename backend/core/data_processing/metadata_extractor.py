"""
metadata_extractor.py
Extracts descriptive metadata from data files for improved cataloging and discovery.
Works with file_handler.py and data_inspector.py to build comprehensive data profiles.
"""

import pandas as pd
import numpy as np
import json
import os
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MetadataExtractor:
    """
    Extracts descriptive metadata from data files including statistics, 
    data fingerprints, and catalog information.
    
    This component serves as the final layer of the data processing pipeline
    before handing data off to System 2 for in-depth analytics.
    """
    
    def __init__(self, max_sample_size: int = 10000):
        """
        Initialize the MetadataExtractor.
        
        Args:
            max_sample_size: Maximum number of rows to sample for metadata extraction
        """
        self.max_sample_size = max_sample_size
    
    def extract_metadata(self, file_path: str, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a data file.
        
        Args:
            file_path: Path to the file
            file_info: Basic file information from DataInspector
            
        Returns:
            Dictionary containing enhanced metadata
        """
        if not os.path.exists(file_path):
            return {"error": "File not found"}
        
        # Start with the file info we already have
        metadata = file_info.copy()
        
        # Add file fingerprint
        metadata["fingerprint"] = self._generate_fingerprint(file_path)
        
        # Add timestamp
        metadata["extraction_timestamp"] = datetime.now().isoformat()
        
        # Process based on file format
        file_extension = file_path.split(".")[-1].lower()
        
        try:
            # For tabular data formats
            if file_extension in ['csv', 'xlsx', 'xls', 'parquet'] or \
               (file_extension == 'json' and metadata.get('structure') != 'nested'):
                
                # Load data - for Excel, use sheet from file_info if available
                if file_extension in ['xlsx', 'xls'] and 'sheet_names' in metadata:
                    sheet_name = metadata['sheet']
                    df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                      nrows=min(self.max_sample_size, metadata.get('row_count', self.max_sample_size)))
                elif file_extension == 'csv':
                    df = pd.read_csv(file_path, nrows=min(self.max_sample_size, metadata.get('row_count', self.max_sample_size)))
                elif file_extension == 'parquet':
                    df = pd.read_parquet(file_path)
                    if len(df) > self.max_sample_size:
                        df = df.sample(self.max_sample_size)
                elif file_extension == 'json':
                    df = pd.read_json(file_path)
                    if len(df) > self.max_sample_size:
                        df = df.sample(self.max_sample_size)
                
                # Extract column statistics
                column_metadata = self._get_column_statistics(df)
                metadata["column_metadata"] = column_metadata
                
                # Extract dataset-level statistics
                metadata["dataset_statistics"] = self._get_dataset_statistics(df)
                
                # Data quality indicators
                metadata["data_quality"] = self._get_data_quality_metrics(df)
                
                # Correlation heatmap data (if numeric columns exist)
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) >= 2:
                    correlations = df[numeric_cols].corr().round(2).to_dict()
                    metadata["correlations"] = correlations
                
                # Extract time-based patterns if datetime columns exist
                date_cols = [col for col in df.columns if 
                            pd.api.types.is_datetime64_any_dtype(df[col]) or
                            (isinstance(col, str) and any(date_term in col.lower() 
                                                         for date_term in ['date', 'time', 'year', 'month', 'day']))]
                
                if date_cols:
                    # Convert potential date columns that weren't auto-detected
                    for col in date_cols:
                        if not pd.api.types.is_datetime64_any_dtype(df[col]):
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                            except:
                                # Remove from date_cols if conversion fails
                                date_cols.remove(col)
                    
                    # Extract time patterns for successfully converted columns
                    if date_cols:
                        metadata["temporal_patterns"] = self._extract_temporal_patterns(df, date_cols)
            
            # For JSON with nested structure
            elif file_extension == 'json' and metadata.get('structure') == 'nested':
                with open(file_path, 'r') as f:
                    json_data = json.load(f)
                
                # Get structure metrics for nested JSON
                metadata["json_structure"] = self._analyze_json_structure(json_data)
            
            # Add data dictionary
            if "columns" in metadata:
                metadata["data_dictionary"] = self._generate_data_dictionary(metadata)
            
            # Add dataset tags
            metadata["tags"] = self._generate_tags(metadata)
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata for {file_path}: {str(e)}")
            metadata["metadata_extraction_error"] = str(e)
            return metadata
    
    def _generate_fingerprint(self, file_path: str) -> str:
        """
        Generate a fingerprint for a file based on content sampling and metadata.
        
        Args:
            file_path: Path to the file
            
        Returns:
            String representation of the fingerprint
        """
        import hashlib
        
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
    
    def _get_column_statistics(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Extract statistics for each column in the DataFrame.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary of column statistics
        """
        result = {}
        
        for column in df.columns:
            col_data = df[column]
            col_stats = {
                "dtype": str(col_data.dtype),
                "count": len(col_data),
                "null_count": col_data.isna().sum(),
                "null_percentage": round((col_data.isna().sum() / len(col_data)) * 100, 2) if len(col_data) > 0 else 0,
                "unique_count": col_data.nunique()
            }
            
            # Add type-specific statistics
            if pd.api.types.is_numeric_dtype(col_data):
                # For numeric columns
                col_stats.update({
                    "min": col_data.min() if not col_data.empty and not col_data.isna().all() else None,
                    "max": col_data.max() if not col_data.empty and not col_data.isna().all() else None,
                    "mean": col_data.mean() if not col_data.empty and not col_data.isna().all() else None,
                    "median": col_data.median() if not col_data.empty and not col_data.isna().all() else None,
                    "std": col_data.std() if not col_data.empty and not col_data.isna().all() else None,
                    "distribution": self._get_numeric_distribution(col_data)
                })
            
            elif pd.api.types.is_string_dtype(col_data):
                # For string columns
                non_null_values = col_data.dropna()
                
                if not non_null_values.empty:
                    # Get value counts for top values
                    value_counts = col_data.value_counts().head(5).to_dict()
                    
                    col_stats.update({
                        "top_values": value_counts,
                        "avg_length": non_null_values.str.len().mean() if non_null_values.any() else 0,
                        "max_length": non_null_values.str.len().max() if non_null_values.any() else 0,
                        "min_length": non_null_values.str.len().min() if non_null_values.any() else 0
                    })
                    
                    # Check if the column might contain categorical data
                    if col_stats["unique_count"] < min(10, len(col_data) * 0.1):
                        col_stats["potential_categorical"] = True
            
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                # For datetime columns
                non_null_dates = col_data.dropna()
                
                if not non_null_dates.empty:
                    col_stats.update({
                        "min_date": non_null_dates.min().isoformat() if not non_null_dates.empty else None,
                        "max_date": non_null_dates.max().isoformat() if not non_null_dates.empty else None,
                        "date_range_days": (non_null_dates.max() - non_null_dates.min()).days 
                                          if not non_null_dates.empty else None
                    })
            
            result[column] = col_stats
        
        return result
    
    def _get_numeric_distribution(self, series: pd.Series, bins: int = 5) -> Dict[str, float]:
        """
        Generate a simple histogram for numeric data.
        
        Args:
            series: Numeric pandas Series
            bins: Number of bins for the histogram
            
        Returns:
            Dictionary with binned distribution
        """
        if series.empty or series.isna().all():
            return {}
        
        # Remove nulls for histogram
        non_null = series.dropna()
        
        if non_null.empty:
            return {}
        
        try:
            # Create histogram
            hist, bin_edges = np.histogram(non_null, bins=bins)
            
            # Format result
            distribution = {}
            for i in range(len(hist)):
                bin_label = f"{bin_edges[i]:.2f} to {bin_edges[i+1]:.2f}"
                distribution[bin_label] = int(hist[i])
            
            return distribution
        except Exception as e:
            logger.warning(f"Could not create histogram: {str(e)}")
            return {}
    
    def _get_dataset_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extract dataset-level statistics.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with dataset statistics
        """
        total_cells = df.size
        missing_cells = df.isna().sum().sum()
        duplicate_rows = df.duplicated().sum()
        
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "total_cells": total_cells,
            "missing_cells": int(missing_cells),
            "missing_percentage": round((missing_cells / total_cells) * 100, 2) if total_cells > 0 else 0,
            "duplicate_rows": int(duplicate_rows),
            "duplicate_percentage": round((duplicate_rows / len(df)) * 100, 2) if len(df) > 0 else 0,
            "memory_usage_bytes": df.memory_usage(deep=True).sum()
        }
    
    def _get_data_quality_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate basic data quality metrics.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with data quality metrics
        """
        # Initialize metrics
        quality_metrics = {
            "completeness": {},
            "potential_issues": []
        }
        
        # Check completeness
        total_cells = df.size
        non_null_cells = df.count().sum()
        quality_metrics["completeness"]["score"] = round((non_null_cells / total_cells) * 100, 2) if total_cells > 0 else 0
        
        # Check for columns with high null percentage
        high_null_cols = []
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100
            if null_pct > 20:  # More than 20% nulls
                high_null_cols.append({
                    "column": col,
                    "null_percentage": round(null_pct, 2)
                })
        
        if high_null_cols:
            quality_metrics["potential_issues"].append({
                "issue_type": "high_missing_values",
                "affected_columns": high_null_cols
            })
        
        # Check for potential outliers in numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        outlier_cols = []
        
        for col in numeric_cols:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            outlier_count = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            outlier_pct = (outlier_count / len(df)) * 100
            
            if outlier_pct > 5:  # More than 5% potential outliers
                outlier_cols.append({
                    "column": col,
                    "outlier_percentage": round(outlier_pct, 2)
                })
        
        if outlier_cols:
            quality_metrics["potential_issues"].append({
                "issue_type": "potential_outliers",
                "affected_columns": outlier_cols
            })
        
        # Check for uniform distribution in categorical columns
        categorical_cols = []
        for col in df.columns:
            # Consider columns with less than 10 unique values as potential categorical
            if df[col].nunique() < min(10, len(df) * 0.1):
                # Check if one value dominates
                value_counts = df[col].value_counts(normalize=True)
                if not value_counts.empty and value_counts.iloc[0] > 0.95:  # If top value > 95%
                    categorical_cols.append({
                        "column": col,
                        "dominant_value": str(value_counts.index[0]),
                        "dominant_value_percentage": round(value_counts.iloc[0] * 100, 2)
                    })
        
        if categorical_cols:
            quality_metrics["potential_issues"].append({
                "issue_type": "unbalanced_categories",
                "affected_columns": categorical_cols
            })
        
        return quality_metrics
    
    def _extract_temporal_patterns(self, df: pd.DataFrame, date_cols: List[str]) -> Dict[str, Any]:
        """
        Extract time-based patterns from datetime columns.
        
        Args:
            df: DataFrame to analyze
            date_cols: List of datetime column names
            
        Returns:
            Dictionary with temporal patterns
        """
        patterns = {}
        
        for col in date_cols:
            if col not in df.columns or not pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
                
            date_data = df[col].dropna()
            if date_data.empty:
                continue
                
            col_patterns = {}
            
            # Get date range
            min_date = date_data.min()
            max_date = date_data.max()
            date_range_days = (max_date - min_date).days
            
            col_patterns["date_range"] = {
                "min": min_date.isoformat(),
                "max": max_date.isoformat(),
                "days": date_range_days
            }
            
            # Distribution by year (if range > 1 year)
            if date_range_days > 365:
                year_dist = date_data.dt.year.value_counts().sort_index().to_dict()
                col_patterns["year_distribution"] = year_dist
            
            # Distribution by month
            month_dist = date_data.dt.month.value_counts().sort_index().to_dict()
            col_patterns["month_distribution"] = {str(k): v for k, v in month_dist.items()}
            
            # Distribution by day of week
            dow_dist = date_data.dt.dayofweek.value_counts().sort_index().to_dict()
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            col_patterns["day_of_week_distribution"] = {days[k]: v for k, v in dow_dist.items()}
            
            patterns[col] = col_patterns
        
        return patterns
    
    def _analyze_json_structure(self, json_data: Any, max_depth: int = 3) -> Dict[str, Any]:
        """
        Analyze the structure of a nested JSON document.
        
        Args:
            json_data: JSON data to analyze
            max_depth: Maximum nesting depth to analyze
            
        Returns:
            Dictionary with JSON structure analysis
        """
        if isinstance(json_data, dict):
            top_level_type = "object"
            children = list(json_data.keys())
        elif isinstance(json_data, list):
            top_level_type = "array"
            children = len(json_data)
        else:
            top_level_type = type(json_data).__name__
            children = None
        
        result = {
            "type": top_level_type,
            "children": children
        }
        
        # Analyze more deeply for objects and arrays if within max depth
        if max_depth > 0:
            if isinstance(json_data, dict):
                child_structures = {}
                for key, value in list(json_data.items())[:10]:  # Limit to first 10 keys
                    child_structures[key] = self._analyze_json_structure(value, max_depth - 1)
                result["properties"] = child_structures
                
            elif isinstance(json_data, list) and len(json_data) > 0:
                # Sample first element if list is not empty
                result["sample_item"] = self._analyze_json_structure(json_data[0], max_depth - 1)
        
        return result
    
    def _generate_data_dictionary(self, metadata: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """
        Generate a data dictionary based on column metadata.
        
        Args:
            metadata: Metadata dictionary containing column information
            
        Returns:
            Dictionary with column descriptions
        """
        data_dictionary = {}
        
        # Use column information from the metadata
        if "columns" in metadata:
            for col_info in metadata["columns"]:
                col_name = col_info["name"]
                data_dictionary[col_name] = {
                    "type": col_info["type"],
                    "description": self._generate_column_description(col_name, col_info),
                    "nullable": "Yes" if col_info.get("nullable", False) else "No"
                }
        
        # Enhance with column metadata if available
        if "column_metadata" in metadata:
            for col_name, col_meta in metadata["column_metadata"].items():
                if col_name in data_dictionary:
                    # Add statistics to description
                    if "unique_count" in col_meta:
                        data_dictionary[col_name]["unique_values"] = col_meta["unique_count"]
                    
                    # Add min/max for numeric columns
                    if "min" in col_meta and "max" in col_meta:
                        data_dictionary[col_name]["range"] = f"{col_meta['min']} to {col_meta['max']}"
                    
                    # Add top values for categorical columns
                    if "top_values" in col_meta:
                        top_values_str = ", ".join([f"{k} ({v})" for k, v in list(col_meta["top_values"].items())[:3]])
                        data_dictionary[col_name]["common_values"] = top_values_str
        
        return data_dictionary
    
    def _generate_column_description(self, column_name: str, column_info: Dict[str, Any]) -> str:
        """
        Generate a descriptive text for a column based on its name and metadata.
        
        Args:
            column_name: Name of the column
            column_info: Column metadata
            
        Returns:
            Description string
        """
        # This is a simple heuristic approach - in a real system, this might use
        # more sophisticated NLP or be enhanced with user-provided descriptions
        
        column_lower = column_name.lower()
        column_type = column_info["type"]
        
        # Try to infer meaning from common column names
        if any(term in column_lower for term in ["id", "identifier", "key"]):
            return f"Unique identifier for {column_lower.replace('_id', '').replace('id', '')}"
        
        elif any(term in column_lower for term in ["date", "time", "created", "updated"]):
            if "created" in column_lower:
                return "Date when the record was created"
            elif "updated" in column_lower or "modified" in column_lower:
                return "Date when the record was last updated"
            else:
                return f"Date/time information for {column_lower}"
        
        elif any(term in column_lower for term in ["name", "title"]):
            return f"Name or title for {column_lower.replace('_name', '').replace('name', '')}"
        
        elif any(term in column_lower for term in ["price", "cost", "amount", "payment"]):
            return f"Financial value representing {column_lower}"
        
        elif any(term in column_lower for term in ["qty", "quantity", "count", "number"]):
            return f"Quantity or count of {column_lower.replace('_count', '').replace('count', '')}"
        
        elif any(term in column_lower for term in ["percentage", "ratio", "rate"]):
            return f"Percentage or ratio value for {column_lower}"
        
        elif any(term in column_lower for term in ["is_", "has_", "can_"]):
            return f"Boolean flag indicating {column_lower.replace('_', ' ')}"
        
        else:
            # Generic description
            return f"{column_name.replace('_', ' ').capitalize()} ({column_type})"
    
    def _generate_tags(self, metadata: Dict[str, Any]) -> List[str]:
        """
        Generate a list of tags for the dataset based on its characteristics.
        
        Args:
            metadata: Dataset metadata
            
        Returns:
            List of descriptive tags
        """
        tags = []
        
        # Add file format tag
        if "format" in metadata:
            tags.append(metadata["format"])
        
        # Add size category
        if "row_count" in metadata:
            row_count = metadata["row_count"]
            if row_count < 1000:
                tags.append("small_dataset")
            elif row_count < 100000:
                tags.append("medium_dataset")
            else:
                tags.append("large_dataset")
        
        # Add tags based on column types
        has_datetime = False
        has_numeric = False
        has_categorical = False
        
        if "column_metadata" in metadata:
            for col, meta in metadata["column_metadata"].items():
                if "dtype" in meta:
                    if "datetime" in meta["dtype"]:
                        has_datetime = True
                    elif any(num_type in meta["dtype"] for num_type in ["int", "float"]):
                        has_numeric = True
                    elif meta.get("potential_categorical", False):
                        has_categorical = True
        
        if has_datetime:
            tags.append("temporal_data")
        if has_numeric:
            tags.append("numeric_data")
        if has_categorical:
            tags.append("categorical_data")
        
        # Add quality tags
        if "data_quality" in metadata:
            quality = metadata["data_quality"]
            completeness = quality.get("completeness", {}).get("score", 0)
            
            if completeness > 90:
                tags.append("high_completeness")
            elif completeness < 70:
                tags.append("low_completeness")
            
            if quality.get("potential_issues", []):
                issue_types = [issue["issue_type"] for issue in quality["potential_issues"]]
                for issue in issue_types:
                    tags.append(f"has_{issue}")
        
        return tags