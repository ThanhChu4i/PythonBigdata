from kafka import KafkaProducer
from dotenv import load_dotenv
import pickle
import gzip
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# Cấu hình Kafka và thư mục chứa file
KAFKA_SERVER = os.getenv("KAFKA_SERVER_PRODUCER")
TOPIC_NAME = os.getenv("TOPIC_NAME")
PICKLE_FILES_DIR = os.getenv("PICKLE_FILES_DIR")

# Khởi tạo Kafka Producer với cấu hình tối ưu
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    acks='all',
    compression_type=None,  # Không nén tại Kafka để tránh lỗi nén
    batch_size=32*1024,
    linger_ms=10,
    buffer_memory=128*1024*1024,
    max_request_size=5*1024*1024,
)

# Hàm gửi file .pickle qua Kafka và giữ nguyên tên file
def send_pickle_file(file_path):
    try:
        filename = os.path.basename(file_path)  # Lấy tên file gốc

        # Đọc và nén dữ liệu file
        with open(file_path, 'rb') as f:
            content = f.read()
            compressed_content = gzip.compress(content)

        # Tạo dữ liệu với tên file và nội dung nén
        message_data = {
            "filename": filename,  # Tên file
            "content": compressed_content  # Nội dung file đã nén
        }
        serialized_data = pickle.dumps(message_data)  # Chuẩn bị dữ liệu để gửi

        # Gửi dữ liệu qua Kafka
        producer.send(TOPIC_NAME, serialized_data)
        logging.info(f"Sent file: {filename} to topic: {TOPIC_NAME}")

    except Exception as e:
        logging.error(f"Error sending file {file_path}: {e}")

# Đọc tất cả file pickle từ thư mục và gửi qua Kafka với nhiều luồng
def main():
    # Tạo ThreadPoolExecutor để chạy các luồng song song
    with ThreadPoolExecutor(max_workers=int(os.getenv("max_worker"))) as executor:  # Tăng số lượng luồng nếu cần
        futures = []
        for filename in os.listdir(PICKLE_FILES_DIR):
            if filename.endswith('.pickle'):
                file_path = os.path.join(PICKLE_FILES_DIR, filename)
                futures.append(executor.submit(send_pickle_file, file_path))

        # Chờ tất cả các luồng hoàn thành
        for future in as_completed(futures):
            future.result()  # Kiểm tra nếu có exception trong các luồng

    # Đảm bảo tất cả message được gửi trước khi đóng producer
    try:
        producer.flush()
        logging.info("All messages sent successfully.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
