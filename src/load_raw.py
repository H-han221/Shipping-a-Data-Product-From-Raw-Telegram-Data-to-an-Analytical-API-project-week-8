import json
import os
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL credentials
DB_HOST = "localhost"
DB_NAME = "medical_telegram"
DB_USER = "postgres"
DB_PASS = "your_password"

# Connect to PostgreSQL
conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
cur = conn.cursor()

# Path to raw JSON
data_dir = "../data/raw/telegram_messages/"

for date_folder in os.listdir(data_dir):
    folder_path = os.path.join(data_dir, date_folder)
    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            file_path = os.path.join(folder_path, file)
            with open(file_path, "r", encoding="utf-8") as f:
                messages = json.load(f)
                rows = []
                for msg in messages:
                    rows.append((
                        msg.get("id"),
                        msg.get("channel"),
                        msg.get("date"),
                        msg.get("text"),
                        msg.get("views", 0),
                        msg.get("forwards", 0),
                        bool(msg.get("media")),
                        msg.get("image_path", None)
                    ))
                execute_values(
                    cur,
                    """
                    INSERT INTO raw.telegram_messages
                    (message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path)
                    VALUES %s
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    rows
                )
                conn.commit()

cur.close()
conn.close()
print("Raw data loaded into PostgreSQL!")
