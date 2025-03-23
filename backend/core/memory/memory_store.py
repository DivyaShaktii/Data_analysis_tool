# backend/core/memory/memory_store.py

"""
Memory Store for maintaining long-term knowledge about data analysis patterns,
insights, and file schemas across sessions. This module implements vector-based
retrieval to find relevant historical information.
"""

import logging
import os
import json
import time
from typing import Dict, List, Any, Optional, Tuple
import threading

# For vector embedding and similarity search
# In a production environment, you would use a proper vector database
# like Pinecone, Weaviate, Milvus, or FAISS
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

class MemoryStore:
    """
    Manages long-term memory storage and retrieval using vector embeddings.
    Provides methods to store and retrieve insights, analysis results, and file schemas.
    
    This implementation uses a simple file-based vector store with TF-IDF.
    For production, consider using a dedicated vector database and better embeddings.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MemoryStore, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        """Initialize the memory store as a singleton"""
        if self._initialized:
            return
            
        self._memory_dir = os.environ.get('MEMORY_DIR', 'data/memory')
        self._insights_file = os.path.join(self._memory_dir, 'insights.json')
        self._schemas_file = os.path.join(self._memory_dir, 'schemas.json')
        self._results_file = os.path.join(self._memory_dir, 'results.json')
        
        # Create memory directory if it doesn't exist
        os.makedirs(self._memory_dir, exist_ok=True)
        
        # Initialize memory stores
        self._insights = self._load_file(self._insights_file, [])
        self._schemas = self._load_file(self._schemas_file, [])
        self._results = self._load_file(self._results_file, [])
        
        # Initialize the vectorizer
        self._vectorizer = TfidfVectorizer(lowercase=True, stop_words='english')
        self._update_vectorizer()
        
        self._initialized = True
    
    def store_insight(self, project_id: str, session_id: Optional[str], 
                     content: str, entities: List[str], context: str) -> None:
        """
        Store an insight in long-term memory.
        
        Args:
            project_id: The project where the insight was generated
            session_id: Optional session where the insight was generated
            content: The text content of the insight
            entities: List of entities related to the insight
            context: Conversation context when the insight was generated
        """
        insight = {
            'project_id': project_id,
            'session_id': session_id,
            'content': content,
            'entities': entities,
            'context': context,
            'timestamp': time.time(),
            'type': 'insight'
        }
        
        self._insights.append(insight)
        self._save_file(self._insights_file, self._insights)
        self._update_vectorizer()
        logger.info(f"Stored new insight from project {project_id}")
    
    def store_file_schema(self, project_id: str, file_id: str, 
                          schema: Dict[str, Any], description: str) -> None:
        """
        Store file schema information in long-term memory.
        
        Args:
            project_id: The project where the file was uploaded
            file_id: Unique identifier for the file
            schema: The file's schema (columns, types, etc.)
            description: A text description of the file
        """
        # Convert schema to a searchable text representation
        columns_text = ", ".join([f"{col}: {schema.get(col, {}).get('type', 'unknown')}" 
                                  for col in schema.keys()])
        
        schema_entry = {
            'project_id': project_id,
            'file_id': file_id,
            'schema': schema,
            'description': description,
            'columns_text': columns_text,
            'text_content': f"{description} {columns_text}",
            'timestamp': time.time(),
            'type': 'schema'
        }
        
        self._schemas.append(schema_entry)
        self._save_file(self._schemas_file, self._schemas)
        self._update_vectorizer()
        logger.info(f"Stored schema for file {file_id} from project {project_id}")
    
    def store_analysis_result(self, project_id: str, task_id: str, task_type: str, 
                             entities: List[str], results: Dict[str, Any]) -> None:
        """
        Store analysis results in long-term memory.
        
        Args:
            project_id: The project where the analysis was performed
            task_id: Unique identifier for the analysis task
            task_type: Type of analysis performed
            entities: List of entities involved in the analysis
            results: The analysis results
        """
        # Create a text representation of the results
        results_text = self._results_to_text(results)
        
        result_entry = {
            'project_id': project_id,
            'task_id': task_id,
            'task_type': task_type,
            'entities': entities,
            'results': results,
            'results_text': results_text,
            'text_content': f"{task_type} analysis on {', '.join(entities)}. {results_text}",
            'timestamp': time.time(),
            'type': 'result'
        }
        
        self._results.append(result_entry)
        self._save_file(self._results_file, self._results)
        self._update_vectorizer()
        logger.info(f"Stored results for task {task_id} from project {project_id}")
    
    def retrieve_relevant_insights(self, query: str, limit: int = 3, 
                                  user_id: Optional[str] = None,
                                  project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve insights relevant to the given query, with optional filtering.
        
        Args:
            query: Text query to match against stored insights
            limit: Maximum number of insights to return
            user_id: Optional user ID to prefer insights from the same user
            project_id: Optional project ID to filter or prioritize insights
            
        Returns:
            List of relevant insights
        """
        relevant_items = self._retrieve_relevant_items(query, limit * 2)
        
        # Filter and prioritize based on project/user if provided
        if project_id or user_id:
            # First get items that match both project and user (if both provided)
            matching_items = []
            other_items = []
            
            for item in relevant_items:
                if project_id and user_id:
                    # For user matching, we'd need to link projects to users
                    # This is a simplification - would need a proper implementation
                    if item.get('project_id') == project_id:
                        matching_items.append(item)
                    else:
                        other_items.append(item)
                elif project_id:
                    if item.get('project_id') == project_id:
                        matching_items.append(item)
                    else:
                        other_items.append(item)
                elif user_id:
                    # We don't have direct user_id in items, so this would 
                    # require additional implementation
                    other_items.append(item)
                
            combined = matching_items + other_items
            return combined[:limit]
        
        # If no filtering, just return top matches
        return relevant_items[:limit]
    
    def retrieve_file_schema(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific file schema by ID.
        
        Args:
            file_id: Unique identifier for the file
            
        Returns:
            File schema if found, None otherwise
        """
        for schema in self._schemas:
            if schema['file_id'] == file_id:
                return schema
        return None
    
    def retrieve_similar_schemas(self, columns: List[str], limit: int = 3) -> List[Dict[str, Any]]:
        """
        Find schemas with similar columns to the provided list.
        
        Args:
            columns: List of column names to match
            limit: Maximum number of schemas to return
            
        Returns:
            List of similar file schemas
        """
        query = ", ".join(columns)
        relevant_items = self._retrieve_relevant_items(query, limit * 2)
        
        # Filter to just schemas
        schemas = [item for item in relevant_items if item['type'] == 'schema']
        return schemas[:limit]
    
    def retrieve_similar_analyses(self, task_type: str, entities: List[str], limit: int = 3) -> List[Dict[str, Any]]:
        """
        Find similar analyses to the specified task and entities.
        
        Args:
            task_type: Type of analysis to match
            entities: Entities involved in the analysis
            limit: Maximum number of results to return
            
        Returns:
            List of similar analysis results
        """
        query = f"{task_type} analysis on {', '.join(entities)}"
        relevant_items = self._retrieve_relevant_items(query, limit * 2)
        
        # Filter to just results
        results = [item for item in relevant_items if item['type'] == 'result']
        return results[:limit]
    
    def _retrieve_relevant_items(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """Find items relevant to the query using vector similarity"""
        if not self._has_vectors():
            return []
        
        # Combine all memory items
        all_items = self._insights + self._schemas + self._results
        if not all_items:
            return []
        
        # Create a vector for the query
        query_vector = self._vectorizer.transform([query])
        
        # Get all item vectors
        item_texts = [item.get('text_content', item.get('content', '')) for item in all_items]
        item_vectors = self._vectorizer.transform(item_texts)
        
        # Calculate similarities
        similarities = cosine_similarity(query_vector, item_vectors)[0]
        
        # Pair items with their similarity scores
        item_scores = list(zip(all_items, similarities))
        
        # Sort by similarity (highest first)
        sorted_items = sorted(item_scores, key=lambda x: x[1], reverse=True)
        
        # Return just the items (without scores), up to the limit
        return [item for item, score in sorted_items[:limit] if score > 0.1]
    
    def _update_vectorizer(self) -> None:
        """Update the vectorizer with all current memory items"""
        # Combine all text content for fitting the vectorizer
        texts = []
        
        # Add insights
        for insight in self._insights:
            texts.append(insight.get('content', ''))
        
        # Add schemas
        for schema in self._schemas:
            texts.append(schema.get('text_content', ''))
        
        # Add results
        for result in self._results:
            texts.append(result.get('text_content', ''))
        
        # Fit the vectorizer if we have texts
        if texts:
            try:
                self._vectorizer.fit(texts)
            except Exception as e:
                logger.error(f"Error updating vectorizer: {str(e)}")
    
    def _has_vectors(self) -> bool:
        """Check if the vectorizer has been fitted with data"""
        try:
            return hasattr(self._vectorizer, 'vocabulary_') and self._vectorizer.vocabulary_
        except:
            return False
    
    def _results_to_text(self, results: Dict[str, Any]) -> str:
        """Convert a results dictionary to a searchable text string"""
        text_parts = []
        
        # Handle different result types differently
        if 'summary' in results:
            text_parts.append(results['summary'])
        
        if 'metrics' in results:
            metrics = results['metrics']
            if isinstance(metrics, dict):
                for key, value in metrics.items():
                    text_parts.append(f"{key}: {value}")
            elif isinstance(metrics, list):
                for item in metrics:
                    if isinstance(item, dict):
                        for key, value in item.items():
                            text_parts.append(f"{key}: {value}")
        
        if 'insights' in results:
            insights = results['insights']
            if isinstance(insights, list):
                for insight in insights:
                    if isinstance(insight, str):
                        text_parts.append(insight)
                    elif isinstance(insight, dict) and 'text' in insight:
                        text_parts.append(insight['text'])
        
        # If nothing specific was found, just use the stringified dict
        if not text_parts:
            text_parts.append(str(results))
        
        return " ".join(text_parts)
    
    def _load_file(self, filepath: str, default: Any) -> Any:
        """Load data from a JSON file or return default if not found"""
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    return json.load(f)
            return default
        except Exception as e:
            logger.error(f"Error loading {filepath}: {str(e)}")
            return default
    
    def _save_file(self, filepath: str, data: Any) -> None:
        """Save data to a JSON file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving to {filepath}: {str(e)}")
    
    def get_all_insights(self) -> List[Dict[str, Any]]:
        """Return all stored insights"""
        return self._insights
    
    def get_all_schemas(self) -> List[Dict[str, Any]]:
        """Return all stored file schemas"""
        return self._schemas
    
    def get_all_results(self) -> List[Dict[str, Any]]:
        """Return all stored analysis results"""
        return self._results
    
    def clear_memory(self, memory_type: Optional[str] = None) -> None:
        """
        Clear memory store data.
        
        Args:
            memory_type: Type of memory to clear ('insights', 'schemas', 'results', or None for all)
        """
        if memory_type == 'insights' or memory_type is None:
            self._insights = []
            self._save_file(self._insights_file, self._insights)
            
        if memory_type == 'schemas' or memory_type is None:
            self._schemas = []
            self._save_file(self._schemas_file, self._schemas)
            
        if memory_type == 'results' or memory_type is None:
            self._results = []
            self._save_file(self._results_file, self._results)
            
        # Re-initialize the vectorizer if we cleared anything
        if memory_type is not None:
            self._update_vectorizer()
            
        logger.info(f"Cleared memory store: {memory_type or 'all'}")
    
    def get_memory_stats(self) -> Dict[str, int]:
        """
        Get statistics about the memory store.
        
        Returns:
            Dictionary with counts of each memory type
        """
        return {
            'insights': len(self._insights),
            'schemas': len(self._schemas),
            'results': len(self._results),
            'total': len(self._insights) + len(self._schemas) + len(self._results)
        }
    
    def prune_old_memories(self, days: int = 30) -> int:
        """
        Remove memories older than the specified number of days.
        
        Args:
            days: Number of days to keep
            
        Returns:
            Number of items removed
        """
        cutoff_time = time.time() - (days * 86400)  # 86400 seconds in a day
        count_before = self.get_memory_stats()['total']
        
        # Filter out old items
        self._insights = [item for item in self._insights if item.get('timestamp', 0) >= cutoff_time]
        self._schemas = [item for item in self._schemas if item.get('timestamp', 0) >= cutoff_time]
        self._results = [item for item in self._results if item.get('timestamp', 0) >= cutoff_time]
        
        # Save updated memories
        self._save_file(self._insights_file, self._insights)
        self._save_file(self._schemas_file, self._schemas)
        self._save_file(self._results_file, self._results)
        
        # Re-initialize the vectorizer
        self._update_vectorizer()
        
        count_after = self.get_memory_stats()['total']
        removed = count_before - count_after
        
        logger.info(f"Pruned {removed} old memories (older than {days} days)")
        return removed
    
    def search_by_keywords(self, keywords: List[str], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search memories by keywords.
        
        Args:
            keywords: List of keywords to search for
            limit: Maximum number of results to return
            
        Returns:
            List of matching memory items
        """
        query = " ".join(keywords)
        return self._retrieve_relevant_items(query, limit)
    
    def find_session_memories(self, session_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Find all memories related to a specific session.
        
        Args:
            session_id: Session ID to search for
            
        Returns:
            Dictionary with insights, schemas, and results lists
        """
        insights = [item for item in self._insights if item.get('session_id') == session_id]
        schemas = [item for item in self._schemas if item.get('session_id') == session_id]
        results = [item for item in self._results if item.get('session_id') == session_id]
        
        return {
            'insights': insights,
            'schemas': schemas,
            'results': results,
            'total_count': len(insights) + len(schemas) + len(results)
        }