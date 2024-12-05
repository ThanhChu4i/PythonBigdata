import sqlite3
import pandas as pd

# Đường dẫn file CSV
csv_file = "/app/data/processed_spotify_tracks_part-00000-75d557b4-f63b-4bd2-b6ab-3028b8df00e1-c000.csv"
db_file = "/app/superset_home/superset.db"
table_name = "Spotifydata"

# Kết nối SQLite
conn = sqlite3.connect(db_file)

# Đọc CSV và nhập vào SQLite
df = pd.read_csv(csv_file)
df.to_sql(table_name, conn, if_exists="replace", index=False)

print(f"Data imported to SQLite table '{table_name}' in {db_file}")
conn.close()
