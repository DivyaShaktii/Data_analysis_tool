### build the docker first
`docker build -t python-sandbox .`

### upload csv files
`curl -X 'POST' 'http://localhost:8000/upload/' -F 'file=@sample.csv'\n`

### execute code in sandbox
`curl -X 'POST' 'http://localhost:8000/execute/?file_path=/tmp/sandbox/1b9e8a45_sample.csv'`

