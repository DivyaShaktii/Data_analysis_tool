import streamlit as st
import pandas as pd
import io
import os
from chatbot import DataAnalyticsBot
import tempfile
import json
from dotenv import load_dotenv
load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="Data Analytics Assistant",
    page_icon="📊",
    layout="wide"
)

# Initialize session state variables
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = []

if "bot" not in st.session_state:
    # Get API key from environment or Streamlit secrets
    #api_key = os.environ.get("ANTHROPIC_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY", None)
    api_key = os.getenv("GROQ_API_KEY")
    
    if not api_key:
        st.error("Anthropic API key not found. Please set it as an environment variable or in Streamlit secrets.")
        st.stop()
    
    st.session_state.bot = DataAnalyticsBot(api_key=api_key)

# Function to handle file upload
def handle_file_upload(uploaded_files):
    for file in uploaded_files:
        # Check if file is already uploaded
        if file.name in [f["name"] for f in st.session_state.uploaded_files]:
            continue
        
        file_info = {
            "name": file.name,
            "type": file.type,
            "size": file.size
        }
        
        # Save file to temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.name)[1]) as tmp_file:
            tmp_file.write(file.getbuffer())
            file_info["path"] = tmp_file.name
        
        # Add file info to session state
        st.session_state.uploaded_files.append(file_info)
        
        # Add file info to bot
        st.session_state.bot.add_file_info(file_info)
        
        # Add file upload message to chat history
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": f"📁 File uploaded: {file.name} ({file.type}, {file.size} bytes)"
        })

# Function to handle user input
def handle_user_input():
    user_input = st.session_state.user_input
    if user_input:
        # Add user message to chat history
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        # Get bot response
        with st.spinner("Thinking..."):
            bot_response = st.session_state.bot.get_response(user_input)
        
        # Add bot response to chat history
        st.session_state.chat_history.append({"role": "assistant", "content": bot_response})
        
        # Clear input box
        st.session_state.user_input = ""

# Sidebar for file uploads and settings
with st.sidebar:
    st.title("Data Analytics Assistant")
    st.markdown("---")
    
    st.subheader("Upload Files")
    uploaded_files = st.file_uploader("Upload data files", accept_multiple_files=True, type=["csv", "xlsx", "json", "txt"])
    
    if uploaded_files:
        handle_file_upload(uploaded_files)
    
    st.markdown("---")
    
    # Display uploaded files
    if st.session_state.uploaded_files:
        st.subheader("Uploaded Files")
        for file_info in st.session_state.uploaded_files:
            st.text(f"📁 {file_info['name']}")
    
    st.markdown("---")
    
    # Add option to clear chat
    if st.button("Clear Chat"):
        st.session_state.chat_history = []
        # Keep file information in the bot but clear conversation
        st.session_state.bot.conversation_history = []
        for file_info in st.session_state.uploaded_files:
            st.session_state.bot.add_file_info(file_info)
    
    # Add option to clear files
    if st.button("Clear Files"):
        # Delete temporary files
        for file_info in st.session_state.uploaded_files:
            if os.path.exists(file_info["path"]):
                os.unlink(file_info["path"])
        
        st.session_state.uploaded_files = []
        # Reinitialize bot to clear file information
        st.session_state.bot = DataAnalyticsBot(api_key=st.session_state.bot.api_key)
        st.session_state.chat_history = []
        st.experimental_rerun()

# Main chat interface
st.title("Data Analytics Chat")

# Display chat messages
chat_container = st.container()
with chat_container:
    for message in st.session_state.chat_history:
        if message["role"] == "user":
            st.markdown(f"**You**: {message['content']}")
        else:
            st.markdown(f"**Assistant**: {message['content']}")
    
    # Add a follow-up question suggestion if there are messages
    if st.session_state.chat_history and st.session_state.chat_history[-1]["role"] == "assistant":
        # Get the last few messages for context
        recent_context = "\n".join([m["content"] for m in st.session_state.chat_history[-4:]])
        
        with st.spinner("Generating follow-up..."):
            followup = st.session_state.bot.ask_followup_question(recent_context)
        
        with st.expander("Suggested follow-up question"):
            st.write(followup)
            if st.button("Ask this question"):
                st.session_state.chat_history.append({"role": "user", "content": followup})
                
                with st.spinner("Thinking..."):
                    bot_response = st.session_state.bot.get_response(followup)
                
                st.session_state.chat_history.append({"role": "assistant", "content": bot_response})
                st.experimental_rerun()

# User input
st.text_input("Ask me about your data:", key="user_input", on_change=handle_user_input)

# Add some helpful information at the bottom
with st.expander("Tips for using the Data Analytics Assistant"):
    st.markdown("""
    - **Upload data files** using the sidebar to analyze them
    - Ask questions about your data like "Summarize this dataset" or "Find correlations between variables"
    - Request visualizations with "Create a chart showing the relationship between X and Y"
    - Use "delegate [task]" to generate a task delegation prompt for complex analyses
    - Check the suggested follow-up questions for ideas on what to ask next
    """)
