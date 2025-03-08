import os
import uuid
import json
import subprocess
import shutil
import tempfile
import asyncio
from typing import Dict, List, Optional, Any 
from fastapi import FastAPI , UploadFile, File, Form,HTTPException, BackgroundTasks, Query, Response
from fastapi.responses import JSONResponse, FileResponse 
from pydantic import BaseModel, field_validator  
import pandas as pd
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import threading
import signal
import uvicorn

asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background task when app starts
    cleanup_task = asyncio.create_task(cleanup_old_jobs())
    yield
    # Cancel the task when app shuts down
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title = "File Processor API",
    description= "API for secure CSV/Excel file processing with Python in a sandboxed environment",
    lifespan=lifespan
)

# Configuration
UPLOAD_FOLDER = '/tmp/file_processor/uploads'
CODE_FOLDER = '/tmp/file_processor/code'
RESULTS_FOLDER = '/tmp/file_processor/results'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}
MAX_EXECUTION_TIME = 120  # seconds
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB limit

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CODE_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

# Job status tracking
job_status: Dict[str, Dict[str, Any]] = {}

class CodeSubmission(BaseModel):
    code: str
    
    @field_validator('code')
    def validate_code(cls, v):
        forbidden_modules = [
            'subprocess', 'os.system', 'eval(', 'exec(', 'importlib', 
            'sys.modules', '__import__', 'open(', 'file(', 
            'execfile(', 'compile(', 'pty', 'popen', 'system'
        ]
        
        for module in forbidden_modules:
            if module in v:
                raise ValueError(f"Forbidden module or function detected: {module}")
        return v

class JobStatus(BaseModel):
    id: str
    filename: str
    status: str
    timestamp: str
    error: Optional[str] = None

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


async def execute_code_in_sandbox(job_id: str, file_path: str, code_path: str, result_path: str):
    """
    Execute Python code in a restricted sandbox environment using Docker.
    """
    try:
        job_status[job_id]['status'] = 'running'
        
        # Get file extension to determine how to load it
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # Prepare Docker command
        docker_cmd = [
            'docker', 'run', '--rm',
            # Set resource limits
            '--memory=512m', '--cpu-shares=512',
            # Set timeout
            '--stop-timeout', str(MAX_EXECUTION_TIME),
            # Mount volumes for file access
            '-v', f"{file_path}:/data/input_file{file_ext}:ro",
            '-v', f"{code_path}:/data/process.py:ro",
            '-v', f"{result_path}:/data/output:rw",
            # Use minimal Python image
            'python:3.9-slim',
            # Run with restricted permissions
            'sh', '-c', f"cd /data && python -m process"
        ]
        
        # Execute in Docker with timeout
        process = await asyncio.create_subprocess_exec(
            *docker_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), 
                timeout=MAX_EXECUTION_TIME
            )
            
            exit_code = process.returncode
            
            if exit_code != 0:
                job_status[job_id]['status'] = 'failed'
                job_status[job_id]['error'] = stderr.decode('utf-8')
            else:
                job_status[job_id]['status'] = 'completed'
                
        except asyncio.TimeoutError:
            # Kill the process if it times out
            if process.returncode is None:
                process.kill()
                
            job_status[job_id]['status'] = 'timeout'
            job_status[job_id]['error'] = f"Execution timed out after {MAX_EXECUTION_TIME} seconds"
            
    except Exception as e:
        job_status[job_id]['status'] = 'failed'
        job_status[job_id]['error'] = str(e)

@app.post("/upload", response_model=Dict[str, str], 
          summary="Upload a CSV or Excel file for processing")

async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file for processing.
    
    Returns a job ID that can be used to submit code and retrieve results.
    """
    # Check if the file is valid
    if file.filename == '':
        raise HTTPException(status_code=400, detail="No file selected")
    
    if not allowed_file(file.filename):
        raise HTTPException(
            status_code=400, 
            detail=f"File type not allowed. Supported formats: {', '.join(ALLOWED_EXTENSIONS)}"
        )
    
    # Create a unique ID for this job
    job_id = str(uuid.uuid4())
    
    # Secure the filename and save the file
    filename = os.path.basename(file.filename)
    file_path = os.path.join(UPLOAD_FOLDER, f"{job_id}_{filename}")
    
    # Save uploaded file
    content = await file.read()
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail=f"File too large. Maximum size: {MAX_UPLOAD_SIZE/1024/1024}MB")
    
    with open(file_path, 'wb') as f:
        f.write(content)
    
    # Initialize job status
    job_status[job_id] = {
        'id': job_id,
        'filename': filename,
        'status': 'uploaded',
        'timestamp': datetime.now().isoformat(),
        'file_path': file_path
    }
    
    return {
        'job_id': job_id,
        'status': 'uploaded',
        'message': 'File uploaded successfully'
    }

@app.post("/submit_code/{job_id}", response_model=Dict[str, str],
          summary="Submit Python code to process the uploaded file")
async def submit_code(
    job_id: str, 
    code_submission: CodeSubmission,
    background_tasks: BackgroundTasks
):
    """
    Submit Python code to process the previously uploaded file.
    
    The code will be executed in a sandboxed Docker container with access to the uploaded file.
    """
    # Check if the job exists
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    try:
        # Create and save the code file
        code_path = os.path.join(CODE_FOLDER, f"{job_id}_process.py")
        with open(code_path, 'w') as code_file:
            code_file.write(code_submission.code)
        
        # Create results directory
        result_path = os.path.join(RESULTS_FOLDER, job_id)
        os.makedirs(result_path, exist_ok=True)
        
        # Update job status
        job_status[job_id]['status'] = 'processing'
        job_status[job_id]['code_path'] = code_path
        job_status[job_id]['result_path'] = result_path
        
        # Execute the code in the background
        file_path = job_status[job_id]['file_path']
        background_tasks.add_task(
            execute_code_in_sandbox,
            job_id, file_path, code_path, result_path
        )
        
        return {
            'job_id': job_id,
            'status': 'processing',
            'message': 'Code submitted and processing started'
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing code: {str(e)}")

@app.get("/status/{job_id}", response_model=Dict[str, Any],
         summary="Check the status of a processing job")
async def get_status(job_id: str):
    """
    Get the current status of a file processing job.
    """
    # Check if the job exists
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_status[job_id]

@app.get("/results/{job_id}", 
         summary="Get the results of a completed processing job")
async def get_results(
    job_id: str, 
    output_format: str = Query("json", description="Output format (json, csv, excel)")
):
    """
    Get the results of a completed file processing job.
    
    The results can be retrieved in different formats: JSON, CSV, or Excel.
    """
    # Check if the job exists
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Check if job is completed
    if job_status[job_id]['status'] != 'completed':
        return JSONResponse(
            status_code=400,
            content={
                'status': job_status[job_id]['status'],
                'message': 'Results not available yet'
            }
        )
    
    result_path = job_status[job_id]['result_path']
    
    # Check if results exist
    result_files = [f for f in os.listdir(result_path) if os.path.isfile(os.path.join(result_path, f))]
    
    if not result_files:
        raise HTTPException(status_code=404, detail="No results found")
    
    # Return based on requested format
    if output_format == 'csv':
        # Find CSV files
        csv_files = [f for f in result_files if f.endswith('.csv')]
        if csv_files:
            return FileResponse(
                path=os.path.join(result_path, csv_files[0]),
                media_type='text/csv',
                filename=f"result_{job_id}.csv"
            )
    
    elif output_format == 'excel':
        # Find Excel files
        excel_files = [f for f in result_files if f.endswith(('.xlsx', '.xls'))]
        if excel_files:
            return FileResponse(
                path=os.path.join(result_path, excel_files[0]),
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                filename=f"result_{job_id}.xlsx"
            )
    
    # Default: return JSON
    json_files = [f for f in result_files if f.endswith('.json')]
    if json_files:
        with open(os.path.join(result_path, json_files[0]), 'r') as f:
            return JSONResponse(content=json.load(f))
    
    # If no specific format file found, return the first result
    return FileResponse(
        path=os.path.join(result_path, result_files[0]),
        filename=f"result_{job_id}_{result_files[0]}"
    )

@app.delete("/cleanup/{job_id}", response_model=Dict[str, str],
            summary="Clean up files and data associated with a job")
async def cleanup_job(job_id: str):
    """
    Delete all files and data associated with a job.
    """
    # Check if the job exists
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Clean up files
    try:
        if 'file_path' in job_status[job_id] and os.path.exists(job_status[job_id]['file_path']):
            os.remove(job_status[job_id]['file_path'])
        
        if 'code_path' in job_status[job_id] and os.path.exists(job_status[job_id]['code_path']):
            os.remove(job_status[job_id]['code_path'])
        
        if 'result_path' in job_status[job_id] and os.path.exists(job_status[job_id]['result_path']):
            shutil.rmtree(job_status[job_id]['result_path'])
        
        # Remove from job status
        del job_status[job_id]
        
        return {'message': f'Job {job_id} cleaned up successfully'}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cleaning up job: {str(e)}")

@app.get("/template", response_model=Dict[str, str],
         summary="Get a code template for file processing")
async def get_template():
    """
    Get a template Python code for processing files.
    """
    template_code = """
# This is a template for processing your data file
# The input file is available as "input_file.csv" (or .xlsx/.xls)
# You should output your results to the current directory

import pandas as pd
import json

# Determine file type and read accordingly
import os
file_extension = os.path.splitext('/data/input_file*')[1].lower()

if file_extension in ['.csv']:
    df = pd.read_csv('/data/input_file.csv')
elif file_extension in ['.xlsx', '.xls']:
    df = pd.read_excel('/data/input_file' + file_extension)
else:
    raise ValueError(f"Unsupported file format: {file_extension}")

# Process your data here
# Example: Calculate summary statistics
result = {
    'row_count': len(df),
    'column_count': len(df.columns),
    'columns': list(df.columns),
    'summary': df.describe().to_dict()
}

# Save results
# Option 1: Save as JSON
with open('/data/output/result.json', 'w') as f:
    json.dump(result, f)

# Option 2: Save as CSV
df.to_csv('/data/output/processed_data.csv', index=False)

# Option 3: Save as Excel
df.to_excel('/data/output/processed_data.xlsx', index=False)

print("Processing completed successfully")
"""
    return {'template': template_code}

# Scheduled cleanup of old jobs
async def cleanup_old_jobs():
    while True:
        try:
            cutoff_time = datetime.now() - timedelta(days=1)
            jobs_to_delete = []
            
            for job_id, job in job_status.items():
                job_time = datetime.fromisoformat(job['timestamp'])
                if job_time < cutoff_time:
                    jobs_to_delete.append(job_id)
            
            for job_id in jobs_to_delete:
                try:
                    if 'file_path' in job_status[job_id] and os.path.exists(job_status[job_id]['file_path']):
                        os.remove(job_status[job_id]['file_path'])
                    
                    if 'code_path' in job_status[job_id] and os.path.exists(job_status[job_id]['code_path']):
                        os.remove(job_status[job_id]['code_path'])
                    
                    if 'result_path' in job_status[job_id] and os.path.exists(job_status[job_id]['result_path']):
                        shutil.rmtree(job_status[job_id]['result_path'])
                    
                    del job_status[job_id]
                except:
                    pass
                    
            # Sleep for 1 hour before next cleanup
            await asyncio.sleep(3600)
        except:
            # If cleanup fails, try again later
            await asyncio.sleep(3600)



if __name__ == "__main__":
    uvicorn.run("scr:app", host="0.0.0.0", port=8000, reload=True)