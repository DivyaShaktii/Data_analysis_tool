import streamlit as st
import requests
import pandas as pd
import json
import os
from io import StringIO

# Configure API endpoint
API_BASE_URL = "http://localhost:8000"  # Adjust to your FastAPI server URL

st.title("Data Analyst Agentic Tool")

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = None
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []
if 'selected_file' not in st.session_state:
    st.session_state.selected_file = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Sidebar with upload and session management
with st.sidebar:
    st.header("Data Management")
    
    # File upload
    uploaded_file = st.file_uploader("Upload Data File", type=["csv", "xlsx", "json"])
    
    if uploaded_file is not None:
        if st.button("Process File"):
            # Prepare for upload
            files = {"file": uploaded_file}
            params = {}
            if st.session_state.session_id:
                params["session_id"] = st.session_state.session_id
            
            # Upload file to backend
            try:
                response = requests.post(
                    f"{API_BASE_URL}/data/upload",
                    files=files,
                    params=params
                )
                
                if response.status_code == 200:
                    result = response.json()
                    st.session_state.session_id = result["session_id"]
                    st.session_state.uploaded_files.append(result)
                    st.success(f"File uploaded: {result['filename']}")
                else:
                    st.error(f"Upload failed: {response.text}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    # Session info
    if st.session_state.session_id:
        st.info(f"Current Session: {st.session_state.session_id}")
        
        if st.button("Refresh Session"):
            try:
                response = requests.get(
                    f"{API_BASE_URL}/data/sessions/{st.session_state.session_id}"
                )
                if response.status_code == 200:
                    session_info = response.json()
                    st.session_state.uploaded_files = session_info.get("files", [])
                    st.success("Session refreshed")
                else:
                    st.error(f"Failed to refresh session: {response.text}")
            except Exception as e:
                st.error(f"Error: {str(e)}")

# Main content area
tab1, tab2 = st.tabs(["Data Explorer", "Chat Assistant"])

# Data Explorer Tab
with tab1:
    st.header("Data Explorer")
    
    if st.session_state.uploaded_files:
        # File selection
        file_names = [file["filename"] for file in st.session_state.uploaded_files]
        selected_file = st.selectbox("Select File", file_names)
        
        if selected_file:
            st.session_state.selected_file = selected_file
            
            # Get data preview
            try:
                preview_rows = st.slider("Preview Rows", min_value=5, max_value=100, value=10)
                response = requests.get(
                    f"{API_BASE_URL}/data/preview/{st.session_state.session_id}/{selected_file}",
                    params={"rows": preview_rows}
                )
                
                if response.status_code == 200:
                    preview_data = response.json()
                    
                    # Display data preview
                    if "data" in preview_data:
                        df = pd.DataFrame(preview_data["data"])
                        st.dataframe(df)
                        
                        # Display basic statistics
                        if st.checkbox("Show Statistics"):
                            st.subheader("Basic Statistics")
                            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                            if not numeric_cols.empty:
                                st.dataframe(df[numeric_cols].describe())
                            else:
                                st.info("No numeric columns available for statistics")
                    else:
                        st.warning("No preview data available")
                else:
                    st.error(f"Failed to load preview: {response.text}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    else:
        st.info("Upload a file to start exploring data")

# Chat Assistant Tab
with tab2:
    st.header("Chat with Your Data")
    
    if st.session_state.session_id and st.session_state.selected_file:
        # Display chat history
        for message in st.session_state.chat_history:
            if message["role"] == "user":
                st.markdown(f"**You:** {message['content']}")
            else:
                st.markdown(f"**Assistant:** {message['content']}")
        
        # Message input
        user_message = st.text_area("Type your question about the data:", height=100)
        
        if st.button("Send"):
            if user_message:
                # Add user message to history
                st.session_state.chat_history.append({
                    "role": "user",
                    "content": user_message
                })
                
                # Send message to backend
                try:
                    response = requests.post(
                        f"{API_BASE_URL}/chat/message",
                        json={
                            "session_id": st.session_state.session_id,
                            "message": user_message,
                            "context": {
                                "current_file": st.session_state.selected_file
                            }
                        }
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        # Add assistant message to history
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": result["response"]
                        })
                        st.experimental_rerun()
                    else:
                        st.error(f"Chat failed: {response.text}")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    else:
        st.info("Upload and select a file to chat about your data")