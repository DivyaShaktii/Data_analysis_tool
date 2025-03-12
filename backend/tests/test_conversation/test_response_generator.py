import pytest
from core.conversation.response_generator import ResponseGenerationAgent
from unittest.mock import Mock

# Mock LLM provider
class MockLLM:
    async def generate(self, prompt, temperature=0.7):
        return "This is a mock response based on your query."

@pytest.fixture
def response_agent():
    llm = MockLLM()
    return ResponseGenerationAgent(llm)

# Test data fixtures
@pytest.fixture
def sample_intent():
    return {
        "type": "data_analysis",
        "confidence": 0.9
    }

@pytest.fixture
def sample_entities():
    return [
        {"type": "column_name", "value": "sales"},
        {"type": "operation", "value": "average"},
        {"type": "time_period", "value": "last month"}
    ]

@pytest.fixture
def sample_conversation_history():
    return [
        {
            "message": "What were our total sales last month?",
            "response": "Let me analyze the sales data for last month."
        },
        {
            "message": "Can you break it down by region?",
            "response": "Here's the sales breakdown by region..."
        }
    ]

@pytest.fixture
def sample_insights():
    return [
        {
            "summary": "Sales increased by 15% compared to previous month",
            "details": "Growth observed across all regions, particularly in North"
        },
        {
            "summary": "Customer satisfaction score: 4.5/5",
            "details": "Improvement in service quality noted"
        }
    ]

@pytest.fixture
def sample_pending_tasks():
    return [
        {
            "task_type": "data_analysis",
            "status": "in_progress",
            "description": "Analyzing regional sales patterns"
        }
    ]

# Test cases
@pytest.mark.asyncio
async def test_generate_basic_response(response_agent, sample_intent, sample_entities):
    """Test basic response generation with minimal inputs"""
    response = await response_agent.generate(
        intent=sample_intent,
        entities=sample_entities,
        conversation_history=[]
    )
    assert isinstance(response, str)
    assert len(response) > 0

@pytest.mark.asyncio
async def test_generate_with_full_context(
    response_agent,
    sample_intent,
    sample_entities,
    sample_conversation_history,
    sample_insights,
    sample_pending_tasks
):
    """Test response generation with all available context"""
    response = await response_agent.generate(
        intent=sample_intent,
        entities=sample_entities,
        conversation_history=sample_conversation_history,
        available_insights=sample_insights,
        pending_tasks=sample_pending_tasks
    )
    assert isinstance(response, str)
    assert len(response) > 0

def test_format_history(response_agent, sample_conversation_history):
    """Test conversation history formatting"""
    formatted = response_agent._format_history(sample_conversation_history)
    assert "User:" in formatted
    assert "Assistant:" in formatted
    assert "Recent conversation history:" in formatted

def test_format_history_empty(response_agent):
    """Test conversation history formatting with empty history"""
    formatted = response_agent._format_history([])
    assert formatted == "No prior conversation."

def test_format_insights(response_agent, sample_insights):
    """Test insights formatting"""
    formatted = response_agent._format_insights(sample_insights)
    assert "Relevant insights" in formatted
    assert "Sales increased" in formatted
    assert "Details:" in formatted

def test_format_insights_empty(response_agent):
    """Test insights formatting with no insights"""
    formatted = response_agent._format_insights(None)
    assert formatted == "No relevant insights available."

def test_format_tasks(response_agent, sample_pending_tasks):
    """Test pending tasks formatting"""
    formatted = response_agent._format_tasks(sample_pending_tasks)
    assert "Pending analytical tasks:" in formatted
    assert "Task: data_analysis" in formatted
    assert "Status: in_progress" in formatted

def test_format_tasks_empty(response_agent):
    """Test tasks formatting with no tasks"""
    formatted = response_agent._format_tasks(None)
    assert formatted == "No pending analytical tasks."

def test_format_entities(response_agent, sample_entities):
    """Test entities formatting"""
    formatted = response_agent._format_entities(sample_entities)
    assert "Identified entities:" in formatted
    assert "column_name: sales" in formatted
    assert "operation: average" in formatted

def test_format_entities_empty(response_agent):
    """Test entities formatting with no entities"""
    formatted = response_agent._format_entities([])
    assert formatted == "No specific entities identified."

# Example of how to run a complete flow test
@pytest.mark.asyncio
async def test_complete_response_flow():
    """Test a complete response generation flow with realistic data"""
    # Setup
    llm = MockLLM()
    agent = ResponseGenerationAgent(llm)
    
    # Test data
    intent = {
        "type": "data_query",
        "confidence": 0.95
    }
    
    entities = [
        {"type": "metric", "value": "revenue"},
        {"type": "timeframe", "value": "Q2 2023"}
    ]
    
    conversation_history = [
        {
            "message": "How is our revenue trending?",
            "response": "Revenue has shown positive growth over the past quarter."
        }
    ]
    
    insights = [
        {
            "summary": "Q2 2023 revenue exceeded targets by 12%",
            "details": "Strong performance in enterprise segment"
        }
    ]
    
    tasks = [
        {
            "task_type": "trend_analysis",
            "status": "queued",
            "description": "Analyzing revenue trends for Q2 2023"
        }
    ]
    
    # Execute
    response = await agent.generate(
        intent=intent,
        entities=entities,
        conversation_history=conversation_history,
        available_insights=insights,
        pending_tasks=tasks
    )
    
    # Verify
    assert isinstance(response, str)
    assert len(response) > 0