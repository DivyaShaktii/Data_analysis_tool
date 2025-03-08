FROM python:3.9-slim

WORKDIR /data

RUN pip install pandas

COPY transform.py /data/transform.py

CMD ["python3", "/data/transform.py"]
