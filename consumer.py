from kafka import KafkaConsumer
from dotenv import load_dotenv
import gzip
import pickle
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cấu hình Kafka và thư mục lưu trữ
KAFKA_SERVER = os.getenv("KAFKA_SERVER_CONSUMER")
TOPIC_NAME = os.getenv("TOPIC_NAME")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
MAX_WORKERS = int(os.getenv("max_worker", 10))

if not all([KAFKA_SERVER, TOPIC_NAME, OUTPUT_DIR]):
    logging.error("Missing environment variables. Please check the .env file.")
    exit(1)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    group_id='pickle_consumer_group',
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    max_poll_records=50,
    fetch_max_bytes=10*1024*1024,
    heartbeat_interval_ms=3000,
    session_timeout_ms=10000,
    consumer_timeout_ms=10000,
)

def process_message(message, output_dir):
    try:
        serialized_data = message.value
        file_data = pickle.loads(serialized_data)
        original_filename = file_data["filename"]
        compressed_content = file_data["content"]

        if not os.path.isdir(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, original_filename)
        with open(output_file, 'wb') as f:
            f.write(gzip.decompress(compressed_content))

        logging.info(f"Saved file: {output_file}")
    except Exception as e:
        logging.error(f"Error processing message {message.offset}: {e}")

def main():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            while True:
                messages = consumer.poll(timeout_ms=1000)

                futures = [
                    executor.submit(process_message, message, OUTPUT_DIR)
                    for batch in messages.values()
                    for message in batch
                ]

                for future in as_completed(futures):
                    future.result()

                # Commit offset sau khi xử lý thành công
                consumer.commit()
                logging.info("Committed offsets.")

        except Exception as e:
            logging.error(f"Consumer error: {e}")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
