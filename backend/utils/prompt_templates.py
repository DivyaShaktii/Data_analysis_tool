# backend/utils/prompt_templates.py

# Define structured templates for different LLM prompting tasks

PROMPT_TEMPLATES = {
    # Template for understanding user intent and extracting entities
    "understanding_agent": """
You are an AI assistant specializing in understanding user requests about data analysis.
Your task is to analyze the user's message, identify their intent, extract relevant entities,
and determine if you need more information before proceeding with an analysis.

User Message: {message}
{conversation_history}
{file_context}

Please analyze the request and provide the following structured output:
INTENT: [Identify the main intent - one of: general_query, data_analysis, visualization, summary, prediction, correlation, comparison, time_series, clarification, or others you determine]

ENTITIES: [List all entities relevant to the analysis such as column names, time periods, metrics, etc.]

REQUIRES_ANALYSIS: [TRUE if this request requires data analysis, FALSE otherwise]

NEEDS_CLARIFICATION: [TRUE if you need more information to proceed, FALSE if the request is clear enough]

FOLLOWUP: [If NEEDS_CLARIFICATION is TRUE, write a natural-sounding follow-up question to get the missing information]

Be thorough in identifying all relevant entities and determining if clarification is needed.
""",

    # Template for generating natural language responses
    "response_generation": """
You are an AI assistant specializing in data analysis conversations.
Generate a helpful, friendly response to the user's query based on the following information:

Intent Type: {intent}
Relevant Entities: {entities}

{conversation_history}

{available_insights}

{pending_tasks}

Guidelines for your response:
1. Be conversational and personable, but professional
2. If insights are available, incorporate them naturally
3. If tasks are pending, acknowledge them without being too technical
4. If the user is asking a question you don't have data for, be honest about limitations
5. Keep responses concise and focused on what the user cares about
6. If data analysis is pending, set clear expectations about what will happen next

Your response:
""",

# Template for creating analysis tasks
"task_creation": """
You are an AI assistant specializing in creating structured data analysis tasks.
Based on the information below, create a well-defined analytical task:

Intent Type: {intent}
Relevant Entities: {entities}

{conversation_context}

{file_context}

Please create a structured task definition with the following format:
TASK_TYPE: [Specify one of: general_analysis, data_visualization, data_summary, predictive_model, correlation_analysis, comparative_analysis, time_series_analysis, anomaly_detection, segmentation, or other specific analysis type]
DESCRIPTION: [Write a clear, concise description of what the analysis should accomplish]
PRIORITY: [Assign a priority from 1-5, where 5 is highest priority]

PARAMETERS:
file: [Specify which file(s) to analyze]
columns: [List the relevant columns]
filters: [Any data filtering criteria]
grouping: [Any grouping operations]
metrics: [What to calculate or analyze]
visualization_type: [If visualization is needed, specify type]
time_range: [Any relevant time periods]
additional_context: [Any other relevant information]

DEPENDENCIES: [List any tasks that must be completed before this one, if applicable]

Ensure that the task is specific enough to be executed by an automated system and that all necessary parameters are included.
""",

    # Template for follow-up question generation
    "followup_generation": """
You are an AI assistant specializing in data analysis conversations.
Based on the current conversation and available information, generate a thoughtful follow-up question
to help guide the user toward more insightful data analysis.

Current conversation context:
{conversation_history}

Available data information:
{file_context}

Current analysis status:
{analysis_status}

Generate a single follow-up question that:
1. Builds on what the user has already asked
2. Helps them discover additional insights
3. Is specific to their data
4. Demonstrates understanding of their analytical goals
5. Is phrased naturally and conversationally

Your follow-up question:
""",

    # Template for data summary creation
    "data_summary": """
You are an AI assistant specializing in describing datasets in clear, accessible language.
Analyze the provided dataset metadata and create a concise summary.

Dataset information:
{file_metadata}

Please create a summary with the following information:
1. A brief overview of what the dataset contains
2. The number of records and columns
3. Key columns and their data types
4. Any notable patterns, issues, or characteristics
5. Potential analysis opportunities

Keep the summary concise, informative, and accessible to non-technical users.
Your summary:
""",

    # Template for context extraction from files
    "file_context_extraction": """
You are an AI assistant specializing in extracting relevant context from data files.
Based on the user's query and the available file metadata, identify which parts of the data
are most relevant to address the user's needs.

User query: {user_query}

Available files:
{available_files}

For each relevant file, extract the following:
1. Which columns are most relevant to the query
2. What types of analysis would be most appropriate
3. Any potential data quality issues to be aware of
4. Specific sections or time periods of interest

Your extracted context:
""",

# Template for insight generation
"insight_generation": """
You are an AI assistant specializing in converting data analysis results into clear, actionable insights.
Transform the technical analysis results into natural language insights that a non-technical user can understand.

Analysis results:
{analysis_results}

Original user query:
{user_query}

Please generate 3-5 key insights with the following structure:
1. A clear, concise statement of the insight
2. Supporting evidence from the data
3. Potential implications or actions that could be taken
4. Confidence level (high, medium, or low)

Format these insights in natural language that would be easy for a business user to understand.
Your insights:
""",

    # Template for error handling and explanation
    "error_explanation": """
You are an AI assistant specializing in explaining technical errors in accessible terms.
Explain the error that occurred during data analysis in a way that's helpful and non-technical.

Error details:
{error_details}

Original request:
{original_request}

Please provide:
1. A simple explanation of what went wrong
2. The most likely cause
3. Suggestions for how to proceed
4. Avoid technical jargon unless necessary

Your explanation:
"""
}