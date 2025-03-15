import streamlit as st
import requests
import pandas as pd
import json
import time
import uuid
from datetime import datetime
import os
from io import StringIO

# Configuration
API_URL = "http://localhost:8000"  # Update this with your API URL

# Set page config
st.set_page_config(
    page_title="Data Analyst Agent",
    page_icon="📊",
    layout="wide",
)

# Initialize session state
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "user_id" not in st.session_state :
    st.session_state.user_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []
if "tasks" not in st.session_state:
    st.session_state.tasks = []
if "file_uploaded" not in st.session_state:
    st.session_state.file_uploaded = False
if "file_info" not in st.session_state:
    st.session_state.file_info = None

# Helper functions
def send_message(message):
    """Send a message to the backend API"""
    url = f"{API_URL}/api/conversation/message"
    data = {
        "session_id": st.session_state.session_id,
        "user_id" : st.session_state.user_id,
        "message": message,
    }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error sending message: {response.text}")
        return None

def upload_file(file):
    """Upload a file to the backend API"""
    url = f"{API_URL}/api/data/upload"
    files = {"file": file}
    data = {"session_id": st.session_state.session_id}
    response = requests.post(url, files=files, data=data)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error uploading file: {response.text}")
        return None

def get_data_preview():
    """Get a preview of the uploaded data"""
    url = f"{API_URL}/api/data/preview/{st.session_state.session_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error getting data preview: {response.text}")
        return None

def get_data_info():
    """Get information about the uploaded data"""
    url = f"{API_URL}/api/data/info/{st.session_state.session_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error getting data info: {response.text}")
        return None

def fetch_conversation_history():
    """Fetch conversation history from the backend API"""
    url = f"{API_URL}/api/conversation/history/{st.session_state.session_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()["history"]
    else:
        st.error(f"Error fetching conversation history: {response.text}")
        return []

def fetch_tasks():
    """Fetch all tasks for the current session"""
    # In a real application, we'd need an endpoint to fetch all tasks by session_id
    # For now, we'll use our session state to track tasks
    updated_tasks = []
    for task in st.session_state.tasks:
        url = f"{API_URL}/api/task/{task['task_id']}"
        response = requests.get(url)
        if response.status_code == 200:
            updated_tasks.append(response.json())
    return updated_tasks

# UI Components
def display_header():
    """Display the app header"""
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📊 Data Analyst Agent")
    with col2:
        st.text(f"Session ID: {st.session_state.session_id[:8]}...")

def display_file_upload():
    """Display the file upload widget"""
    with st.expander("Upload Data", expanded=not st.session_state.file_uploaded):
        uploaded_file = st.file_uploader(
            "Choose a CSV or Excel file",
            type=["csv", "xlsx", "xls"],
            key="file_uploader"
        )
        
        if uploaded_file is not None and not st.session_state.file_uploaded:
            file_details = {"FileName": uploaded_file.name, "FileType": uploaded_file.type, "FileSize": uploaded_file.size}
            st.write(file_details)
            
            if st.button("Upload and Analyze"):
                with st.spinner("Uploading and analyzing file..."):
                    response = upload_file(uploaded_file)
                    if response:
                        st.session_state.file_uploaded = True
                        st.session_state.file_info = response["file_info"]
                        st.success(f"File {uploaded_file.name} uploaded successfully!")
                        # Add system message
                        st.session_state.messages.append({
                            "role": "system",
                            "content": f"File {uploaded_file.name} uploaded successfully. You can now ask questions about the data."
                        })
                        # Get data preview
                        preview = get_data_info()
                        if preview:
                            st.session_state.file_info = preview
                        # Refresh the page to show the updated state
                        st.rerun()

def display_data_preview():
    """Display basic information about the uploaded file"""
    if st.session_state.file_uploaded and st.session_state.file_info:
        with st.expander("File Information", expanded=True):
            # Get the first file in the context
            if isinstance(st.session_state.file_info, dict):
                # If file_info is a direct file object
                file_info = st.session_state.file_info
                filename = file_info.get("filename", "Unknown file")
                metadata = file_info.get("metadata", {})
                format_info = file_info.get("format_info", {})
            else:
                # If file_info is a dictionary of files
                file_id = next(iter(st.session_state.file_info))
                file_data = st.session_state.file_info[file_id]
                filename = file_data.get("metadata", {}).get("filename", "Unknown file")
                metadata = file_data.get("metadata", {}).get("metadata", {})
                format_info = file_data.get("metadata", {}).get("format_info", {})
            
            st.subheader(f"File: {filename}")
            
            # Display file metadata
            st.subheader("File Metadata")
            
            # Display key metadata in a more readable format
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**File Name:** {metadata.get('file_name', 'N/A')}")
                st.write(f"**File Size:** {metadata.get('file_size_mb', 0)} MB")
                st.write(f"**Format:** {format_info.get('format', 'Unknown')}")
            
            with col2:
                st.write(f"**Last Modified:** {metadata.get('last_modified', 'N/A')}")
                st.write(f"**File Extension:** {metadata.get('file_extension', 'N/A')}")
                st.write(f"**File ID:** {metadata.get('file_id', 'N/A')}")
            
            # Show format information
            st.subheader("Format Information")
            st.json(format_info)
            
            # Add a note about data preview
            st.info("Data preview is disabled to avoid serialization issues. Use the chat interface to ask questions about the data.")

def display_task_queue():
    """Display the task queue"""
    with st.sidebar:
        st.subheader("Task Queue")
        
        # Fetch latest task status
        updated_tasks = fetch_tasks()
        if updated_tasks:
            st.session_state.tasks = updated_tasks
        
        if not st.session_state.tasks:
            st.info("No tasks in queue")
        else:
            for task in st.session_state.tasks:
                with st.expander(f"{task['description']} ({task['status']})", expanded=True):
                    st.write(f"ID: {task['task_id'][:8]}...")
                    st.write(f"Status: {task['status']}")
                    st.write(f"Created: {task['created_at']}")
                    st.write(f"Updated: {task['updated_at']}")
                    
                    if task['status'] == "COMPLETED" and task['results']:
                        st.success("Task completed")
                        st.write("Results:")
                        st.json(task['results'])
                    elif task['status'] == "FAILED":
                        st.error("Task failed")
                    elif task['status'] == "RUNNING":
                        st.info("Task is running...")
                    else:
                        st.warning("Task is queued")

def display_chat():
    """Display the chat interface"""
    st.subheader("Chat with Data Analyst Agent")
    
    # Display chat messages
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.write(message["content"])
    
    # Input for new message
    user_input = st.chat_input("Ask a question about your data...")
    if user_input:
        # Add user message to chat
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Display user message
        with st.chat_message("user"):
            st.write(user_input)
        
        # Get response from API
        with st.spinner("Thinking..."):
            response = send_message(user_input)
            if response:
                # Add assistant message to chat
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": response["response"]
                })
                
                # Display assistant message
                with st.chat_message("assistant"):
                    st.write(response["response"])
                
                # Update task list if new tasks were created
                if "tasks_created" in response and response["tasks_created"]:
                    for task_id in response["tasks_created"]:
                        url = f"{API_URL}/api/task/{task_id}"
                        task_response = requests.get(url)
                        if task_response.status_code == 200:
                            st.session_state.tasks.append(task_response.json())
                
                # Force a refresh
                #st.experimental_rerun()

def main():
    # Set up the sidebar
    st.sidebar.title("Data Analyst Agent")
    st.sidebar.info("Upload a file and chat with the agent to analyze your data.")
    
    # Display task queue in sidebar
    display_task_queue()
    
    # Main content
    display_header()
    display_file_upload()
    
    if st.session_state.file_uploaded:
        display_data_preview()
    
    # Display chat interface
    display_chat()
    
    # Auto-refresh task status every 10 seconds
    if st.session_state.tasks:
        st.sidebar.write("Auto-refreshing task status...")
        time.sleep(10)
        st.rerun()

if __name__ == "__main__":
    main()