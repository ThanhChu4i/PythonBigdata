from kafka import KafkaConsumer
from dotenv import load_dotenv
import gzip
import pickle
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cấu hình Kafka và thư mục lưu trữ
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
TOPIC_NAME = os.getenv("TOPIC_NAME")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")

# Khởi tạo KafkaConsumer với cấu hình tối ưu
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    group_id='pickle_consumer_group',
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    max_poll_records=50,
    fetch_max_bytes=10*1024*1024,
    heartbeat_interval_ms=3000,
    session_timeout_ms=10000,
    consumer_timeout_ms=10000,  # Thêm timeout cho consumer
)

# Hàm xử lý và lưu dữ liệu, bao gồm giữ nguyên tên file
def process_message(message, output_dir):
    try:
        # Dữ liệu từ message.value là serialized pickle chứa dữ liệu file
        serialized_data = message.value

        # Deserialize dữ liệu
        file_data = pickle.loads(serialized_data)

        # Lấy tên file và nội dung từ dữ liệu
        original_filename = file_data["filename"]  # Lấy tên file từ message
        compressed_content = file_data["content"]   # Nội dung đã nén

        # Giải nén nội dung
        content = gzip.decompress(compressed_content)

        output_file = os.path.join(output_dir, original_filename)  # Giữ nguyên tên file gốc

        # Lưu dữ liệu vào file
        with open(output_file, 'wb') as f:
            f.write(content)  # Ghi nội dung đã giải nén vào file
        logging.info(f"Saved data to {output_file}")
    except Exception as e:
        logging.error(f"Error processing message {message.offset}: {e}")

# Tiến hành tiêu thụ message từ Kafka với nhiều luồng
def main():
    with ThreadPoolExecutor(max_workers=int(os.getenv("max_worker"))) as executor:  # Tăng số luồng lên để xử lý nhanh hơn
        try:
            while True:
                messages = consumer.poll(timeout_ms=1000)

                # Đưa từng message vào luồng xử lý song song
                futures = []
                for _, batch in messages.items():
                    for message in batch:
                        futures.append(executor.submit(process_message, message, OUTPUT_DIR))

                # Đảm bảo tất cả các luồng hoàn thành
                for future in as_completed(futures):
                    future.result()
        except Exception as e:
            logging.error(f"Consumer error: {e}")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
