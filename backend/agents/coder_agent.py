from typing import List, Dict, Any
from dataclasses import dataclass

@dataclass
class CodeContext:
    """Class to store the context for code generation"""
    variables: Dict[str, Any]  # Dictionary of variable names and their types/values
    functions: List[Dict[str, str]]  # List of dictionaries containing function info
    code_history: List[str]  # List of previously executed code statements

class CodeWriterAgent:
    def __init__(self):
        self.system_prompt = """You are a Python code generation assistant. 
        Generate code using existing variables and functions when possible.
        Provide clean, efficient, and well-commented code."""
    
    def format_context(self, context: CodeContext) -> str:
        """Format the context information for the prompt"""
        context_str = "Available Variables:\n"
        for var_name, var_info in context.variables.items():
            context_str += f"- {var_name}: {var_info['type']}\n"
        
        context_str += "\nAvailable Functions:\n"
        for func in context.functions:
            context_str += f"- {func['name']}({func['params']}): {func['description']}\n"
        
        context_str += "\nCode History:\n"
        for code in context.code_history:
            context_str += f"- {code}\n"
            
        return context_str

    def generate_code(self, 
                     problem_statement: str,
                     variables: Dict[str, Any] = None,
                     functions: List[Dict[str, str]] = None,
                     code_history: List[str] = None) -> str:
        """
        Generate code based on the problem statement and available context
        
        Args:
            problem_statement (str): Description of the coding task
            variables (Dict[str, Any]): Dictionary of available variables
                Format: {
                    'variable_name': {
                        'type': 'type_name',
                        'description': 'variable description'
                    }
                }
            functions (List[Dict[str, str]]): List of available functions
                Format: [{
                    'name': 'function_name',
                    'params': 'param1, param2, ...',
                    'description': 'function description',
                    'return_type': 'return type'
                }]
            code_history (List[str]): List of previously executed code statements
        
        Returns:
            str: Generated code for the given problem statement
        """
        # Initialize empty containers if None
        variables = variables or {}
        functions = functions or []
        code_history = code_history or []
        
        # Create context object
        context = CodeContext(
            variables=variables,
            functions=functions,
            code_history=code_history
        )
        
        # Format the context information
        context_info = self.format_context(context)
        
        # Here you would integrate with your preferred LLM to generate the code
        # For example, using OpenAI's API or any other LLM
        
        # Example prompt structure
        prompt = f"""
        Problem Statement: {problem_statement}
        
        {context_info}
        
        Generate Python code to solve the problem using available variables and functions when possible.
        """
        
        # TODO: Implement the actual LLM call here
        # generated_code = call_llm(prompt)
        
        return "# Generated code will be returned here"

# Example usage:
if __name__ == "__main__":
    # Example context
    variables = {
        'user_dataframe': {
            'type': 'pandas.DataFrame',
            'description': 'DataFrame containing user information'
        }
    }
    
    functions = [{
        'name': 'process_dataframe',
        'params': 'df, columns',
        'description': 'Processes the dataframe and handles missing values',
        'return_type': 'pandas.DataFrame'
    }]
    
    code_history = [
        "user_dataframe = pd.read_csv('user_data.csv')",
        "user_dataframe.head()"
    ]
    
    agent = CodeWriterAgent()
    code = agent.generate_code(
        problem_statement="Count number of null values in the user_dataframe",
        variables=variables,
        functions=functions,
        code_history=code_history
    )
