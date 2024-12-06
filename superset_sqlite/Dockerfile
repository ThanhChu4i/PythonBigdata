FROM apache/superset:latest

# Chuyển sang người dùng root để cài đặt các phụ thuộc hệ thống
USER root

# Cài đặt các gói hệ thống cần thiết (libpq-dev, gcc) cho psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc

# Chuyển file requirements.txt vào container
COPY requirements.txt /app/requirements.txt
COPY superset_config.py /app/pythonpath/superset_config.py

# Cài đặt các gói Python trong requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Cài đặt thêm các thư viện Python cần thiết (pandas, psycopg2-binary)
RUN pip install --no-cache-dir pandas psycopg2-binary

# Quay lại người dùng Superset
USER superset

# Thiết lập thư mục làm việc
WORKDIR /app
