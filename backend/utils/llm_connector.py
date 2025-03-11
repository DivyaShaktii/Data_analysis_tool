# backend/utils/llm_connector.py
import logging
import os
from typing import Dict, Optional, Any
import json

logger = logging.getLogger(__name__)

class LLMProvider:
    """
    Interface for different LLM providers. This class provides a common
    interface for generating text with different LLM services.
    """
    
    @staticmethod
    def create(provider: str = "openai", model_name: Optional[str] = None) -> 'BaseLLMProvider':
        """
        Factory method to create an instance of the appropriate LLM provider.
        
        Args:
            provider: The LLM provider to use (openai, anthropic, or groq)
            model_name: Optional specific model to use
            
        Returns:
            An instance of the appropriate LLM provider class
        """
        provider = provider.lower()
        
        if provider == "openai":
            return OpenAIProvider(model_name=model_name or "gpt-4")
        elif provider == "anthropic":
            return AnthropicProvider(model_name=model_name or "claude-3-opus-20240229")
        elif provider == "groq":
            return GroqProvider(model_name=model_name or "llama3-70b-8192")
        else:
            logger.warning(f"Unknown provider {provider}, falling back to OpenAI")
            return OpenAIProvider(model_name=model_name or "gpt-4")


class BaseLLMProvider:
    """Base class for LLM providers"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
    
    async def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """
        Generate text based on the prompt.
        
        Args:
            prompt: The input prompt
            temperature: Controls randomness (0.0 to 1.0)
            
        Returns:
            The generated text
        """
        raise NotImplementedError("Subclasses must implement this method")


class OpenAIProvider(BaseLLMProvider):
    """OpenAI API provider implementation"""
    
    def __init__(self, model_name: str = "gpt-4"):
        super().__init__(model_name)
        try:
            import openai
            self.client = openai.AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            logger.info(f"Initialized OpenAI provider with model {model_name}")
        except ImportError:
            logger.error("Failed to import OpenAI library. Please install with 'pip install openai'")
            raise
    
    async def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate text using OpenAI API"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=2000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error generating text with OpenAI: {str(e)}")
            return f"Error generating response: {str(e)}"


class AnthropicProvider(BaseLLMProvider):
    """Anthropic Claude API provider implementation"""
    
    def __init__(self, model_name: str = "claude-3-opus-20240229"):
        super().__init__(model_name)
        try:
            import anthropic
            self.client = anthropic.AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
            logger.info(f"Initialized Anthropic provider with model {model_name}")
        except ImportError:
            logger.error("Failed to import Anthropic library. Please install with 'pip install anthropic'")
            raise
    
    async def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate text using Anthropic Claude API"""
        try:
            response = await self.client.messages.create(
                model=self.model_name,
                max_tokens=2000,
                temperature=temperature,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Error generating text with Anthropic: {str(e)}")
            return f"Error generating response: {str(e)}"


class GroqProvider(BaseLLMProvider):
    """Groq API provider implementation"""
    
    def __init__(self, model_name: str = "llama3-70b-8192"):
        super().__init__(model_name)
        try:
            import groq
            self.client = groq.AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))
            logger.info(f"Initialized Groq provider with model {model_name}")
        except ImportError:
            logger.error("Failed to import Groq library. Please install with 'pip install groq'")
            raise
    
    async def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate text using Groq API"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=2000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error generating text with Groq: {str(e)}")
            return f"Error generating response: {str(e)}"