FROM apache/superset:latest

# Cài đặt thêm các thư viện Python cần thiết (pandas để xử lý CSV)
USER root
RUN pip install pandas

USER superset
WORKDIR /app
