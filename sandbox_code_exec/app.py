from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd
import tempfile
import shutil
import os
import uuid
import subprocess

app = FastAPI()

SANDBOX_DIR = "/tmp/sandbox"
os.makedirs(SANDBOX_DIR, exist_ok=True)

DOCKER_IMAGE = "python-sandbox"  # Define your Docker image name

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Generate a unique filename to avoid conflicts
        unique_id = str(uuid.uuid4())[:8]
        temp_filename = f"{unique_id}_{file.filename}"
        temp_path = os.path.join(SANDBOX_DIR, temp_filename)

        # Save file securely
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        return {"message": "File uploaded successfully", "file_path": temp_path}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute/")
async def execute_code(file_path: str):
    """Runs the transformation script inside a Docker container."""
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        # Define the command to run inside Docker
        command = [
            "docker", "run", "--rm",
            "-v", f"{file_path}:/input_file.csv",  # Mount input file
            "-v", "/tmp/sandbox/output:/output",  # Mount output directory
            DOCKER_IMAGE,  
            "python3", "/data/transform.py", "/input_file.csv", "/output/"  # Pass output dir
        ]

        # Run the command
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)

        return {
            "stdout": result.stdout,
            "stderr": result.stderr
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Execution timeout")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
