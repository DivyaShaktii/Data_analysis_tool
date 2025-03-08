from langchain_groq import ChatGroq
import os
from typing import List, Dict, Any, Optional
import json

 


class DataAnalyticsBot:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Data Analytics Bot with Anthropic Claude."""
        self.api_key = api_key 
        if not self.api_key:
            raise ValueError("Anthropic API key is required. Set it as an environment variable or pass it directly.")
        
        self.client = ChatGroq(temperature=0, groq_api_key=GROQ_API_KEY, model_name="llama3-8b-8192")
        self.conversation_history = []
        self.uploaded_files_info = []
        self.system_prompt = """
        You are a specialized Data Analytics Manager assistant. Your responsibilities include:
        
        1. Analyzing data provided by users
        2. Providing insights and visualizations based on the data
        3. Asking relevant follow-up questions to better understand user needs
        4. Creating task delegation prompts for planning agents when needed
        5. Maintaining context throughout the conversation
        
        When analyzing data:
        - Summarize key statistics and patterns
        - Suggest appropriate visualization methods
        - Identify potential issues or anomalies
        - Recommend next steps for deeper analysis
        
        For task delegation:
        - Break down complex requests into manageable tasks
        - Prioritize tasks based on importance and dependencies
        - Specify required inputs and expected outputs for each task
        
        Always be helpful, clear, and focused on providing actionable insights.
        """
    
    def add_file_info(self, file_info: Dict[str, Any]):
        """Add information about uploaded files."""
        self.uploaded_files_info.append(file_info)
        
        # Add file information to conversation history
        file_message = {
            "role": "assistant",
            "content": f"I've received your file: {file_info['name']}. I'll use this for our data analysis."
        }
        self.conversation_history.append(file_message)
    
    def get_file_summary(self) -> str:
        """Generate a summary of all uploaded files."""
        if not self.uploaded_files_info:
            return "No files have been uploaded yet."
        
        summary = "Files available for analysis:\n"
        for idx, file in enumerate(self.uploaded_files_info, 1):
            summary += f"{idx}. {file['name']} ({file['type']}, {file['size']} bytes)\n"
        
        return summary
    
    def generate_task_delegation_prompt(self, task_description: str) -> str:
        """Generate a prompt for task delegation to a planner agent."""
        prompt = f"""
        # Task Delegation Request
        
        ## Original User Request
        {task_description}
        
        ## Available Data
        {self.get_file_summary()}
        
        ## Required Tasks
        Please break down this data analytics request into:
        1. Data preparation tasks
        2. Analysis tasks
        3. Visualization tasks
        4. Reporting tasks
        
        For each task, specify:
        - Task description
        - Input requirements
        - Expected output
        - Estimated complexity (Low/Medium/High)
        - Dependencies on other tasks
        
        ## Priority and Timeline
        Suggest a logical sequence and priority for these tasks.
        """
        
        return prompt
    
    def get_response(self, user_message: str) -> str:
        """Get a response from the Data Analytics Bot."""
        # Add user message to conversation history
        self.conversation_history.append({"role": "user", "content": user_message})
        
        # Check if this is a task delegation request
        if "delegate" in user_message.lower() or "task delegation" in user_message.lower():
            delegation_prompt = self.generate_task_delegation_prompt(user_message)
            self.conversation_history.append({"role": "assistant", "content": f"I've prepared a task delegation prompt:\n\n{delegation_prompt}"})
            return f"I've prepared a task delegation prompt:\n\n{delegation_prompt}"
        
        # Prepare messages for Claude
        messages = [{"role": "system", "content": self.system_prompt}]
        
        # Add file context if available
        if self.uploaded_files_info:
            file_context = self.get_file_summary()
            messages.append({"role": "assistant", "content": f"Context: {file_context}"})
        
        # Add conversation history (limited to last 10 exchanges to manage context length)
        for message in self.conversation_history[-10:]:
            messages.append({"role": message["role"], "content": message["content"]})
        
        # Get response from Claude
        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            messages=messages
        )
        
        assistant_response = response.content[0].text
        
        # Add assistant response to conversation history
        self.conversation_history.append({"role": "assistant", "content": assistant_response})
        
        return assistant_response
    
    def ask_followup_question(self, context: str) -> str:
        """Generate a follow-up question based on the conversation context."""
        followup_prompt = f"""
        Based on our conversation so far, particularly:
        
        {context}
        
        What would be a helpful follow-up question to ask the user to better understand their data analytics needs?
        
        Generate just one clear, specific question that would help clarify the user's requirements or provide more valuable insights.
        """
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": followup_prompt}
        ]
        
        response = self.client.messages.create(
            model="claude-3-haiku-20240307",  # Using a smaller model for efficiency
            max_tokens=100,
            messages=messages
        )
        
        return response.content[0].text
    
    def get_conversation_history(self) -> List[Dict[str, str]]:
        """Return the conversation history."""
        return self.conversation_history
