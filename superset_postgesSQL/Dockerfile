FROM apache/superset:latest

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev

USER superset
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
