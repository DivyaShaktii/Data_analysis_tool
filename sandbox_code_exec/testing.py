import asyncio
import os
import tempfile





async def execute_code_in_sandbox(code: str):
    """
    Execute Python code in a restricted sandbox environment using Docker and return the output.
    Args:
        code: String containing Python code to execute
        
    Returns:
        dict: Contains execution results with keys for 'status', 'output', and 'error'
    """
    try:
        # Create temporary files for code
        with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as code_file:
            code_file.write(code)
            code_path = code_file.name
        
        # Prepare result dictionary
        result = {
            'status': 'pending',
            'output': '',
            'error': ''
        }
        
        # Prepare Docker command
        docker_cmd = [
            'docker', 'run', '--rm',
            # Set resource limits
            '--memory=512m', '--cpu-shares=512',
            # Set timeout
            '--stop-timeout', '10',  # 10 seconds timeout
            # Mount code file
            '-v', f"{code_path}:/data/script.py:ro",
            # Use minimal Python image
            'python:3.9-slim',
            # Run with restricted permissions
            'python', '/data/script.py'
        ]
        
        # Execute in Docker
        process = await asyncio.create_subprocess_exec(
            *docker_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), 
                timeout=10  # 10 seconds timeout
            )
            
            exit_code = process.returncode
            
            if exit_code != 0:
                result['status'] = 'failed'
                result['error'] = stderr.decode('utf-8')
            else:
                result['status'] = 'completed'
                result['output'] = stdout.decode('utf-8')
                
        except asyncio.TimeoutError:
            # Kill the process if it times out
            if process.returncode is None:
                process.kill()
                
            result['status'] = 'timeout'
            result['error'] = f"Execution timed out after 10 seconds"
            
    except Exception as e:
        result['status'] = 'failed'
        result['error'] = str(e)
    
    finally:
        # Clean up temporary file
        if 'code_path' in locals():
            os.remove(code_path)
    
    return result

async def test_sandbox_execution():
    """
    Test function to demonstrate the use of execute_code_in_sandbox
    """
    # Simple example that prints some text and does basic calculations
    test_code = """
print("Hello from the sandbox!")
print("Performing some calculations...")
result = 0
for i in range(1, 11):
    result += i
print(f"Sum of numbers 1 to 10: {result}")

# Test some basic Python functionality
import math
print(f"Square root of 16: {math.sqrt(16)}")

# Create and manipulate a list
numbers = [1, 2, 3, 4, 5]
squared = [x**2 for x in numbers]
print(f"Original numbers: {numbers}")
print(f"Squared numbers: {squared}")
"""
    
    print("Executing code in sandbox...")
    result = await execute_code_in_sandbox(test_code)
    
    print("\nExecution Status:", result['status'])
    
    if result['status'] == 'completed':
        print("\nOutput:")
        print(result['output'])
    else:
        print("\nError:")
        print(result['error'])
    
    return result

asyncio.run(test_sandbox_execution())