from kafka import KafkaProducer
from dotenv import load_dotenv
import pickle
import gzip
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka.errors import KafkaError

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# Cấu hình Kafka và thư mục chứa file
KAFKA_SERVER = os.getenv("KAFKA_SERVER_PRODUCER")
TOPIC_NAME = os.getenv("TOPIC_NAME")
PICKLE_FILES_DIR = os.getenv("PICKLE_FILES_DIR")
MAX_WORKERS = int(os.getenv("max_worker", 10))

if not all([KAFKA_SERVER, TOPIC_NAME, PICKLE_FILES_DIR]):
    logging.error("Missing environment variables. Please check the .env file.")
    exit(1)

# Khởi tạo Kafka Producer với cấu hình tối ưu
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    acks='all',
    compression_type='gzip',
    batch_size=32*1024,
    linger_ms=50,
    buffer_memory=128*1024*1024,
    max_request_size=5*1024*1024,
)

# Hàm gửi file .pickle qua Kafka
def send_pickle_file(file_path):
    retries = 3
    filename = os.path.basename(file_path)

    for attempt in range(retries):
        try:
            logging.info(f"Processing file: {file_path}")

            with open(file_path, 'rb') as f:
                content = f.read()
                compressed_content = gzip.compress(content)

            message_data = {"filename": filename, "content": compressed_content}
            serialized_data = pickle.dumps(message_data)

            producer.send(TOPIC_NAME, serialized_data).get(timeout=10)
            logging.info(f"Sent file: {filename} to topic: {TOPIC_NAME}")
            break
        except KafkaError as e:
            logging.warning(f"Retry {attempt + 1}/{retries} for file {filename} due to error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error on file {filename}: {e}")
            break

def main():
    if not os.path.exists(PICKLE_FILES_DIR):
        logging.error(f"Directory {PICKLE_FILES_DIR} does not exist.")
        exit(1)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_pickle_file, os.path.join(PICKLE_FILES_DIR, f))
                   for f in os.listdir(PICKLE_FILES_DIR) if f.endswith('.pickle')]

        for future in as_completed(futures):
            future.result()

    try:
        producer.flush()
        logging.info("All messages sent successfully.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
