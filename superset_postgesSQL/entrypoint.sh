#!/bin/sh

# Tạo tài khoản admin
superset fab create-admin --username admin --password admin --firstname Admin --lastname User --email admin@admin.com

# Nâng cấp cơ sở dữ liệu
superset db upgrade

# Khởi tạo Superset
superset init

# Chạy Superset
superset run -h 0.0.0.0 -p 8088
