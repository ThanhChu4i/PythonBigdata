FROM apache/superset:latest

# Cài đặt thêm thư viện (nếu cần)
USER root
RUN pip install pandas
RUN pip install psycopg2-binary

# Sao chép script vào container và thêm quyền thực thi
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER superset
WORKDIR /app
