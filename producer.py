from kafka import KafkaProducer
import pickle
import os
import logging

# Thiết lập logging để theo dõi lỗi và thông tin
logging.basicConfig(level=logging.INFO)

def send_pickle_to_kafka(pickle_folder, topic, kafka_server='localhost:9092'):
    # Khởi tạo Kafka Producer với giá trị dưới dạng byte
    producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda v: pickle.dumps(v))

    for filename in os.listdir(pickle_folder):
        if filename.endswith('.pickle'):
            filepath = os.path.join(pickle_folder, filename)
            try:
                with open(filepath, 'rb') as f:
                    data = pickle.load(f)
                    # Gửi dữ liệu qua Kafka
                    producer.send(topic, value=data)
                    logging.info(f"Sent file {filename} to topic {topic}")
            except Exception as e:
                logging.error(f"Failed to send file {filename}: {e}")
    
    producer.flush()  # Đảm bảo tất cả các tin nhắn đã được gửi
    producer.close()  # Đóng kết nối Kafka producer
    logging.info("All data has been sent to Kafka.")

# Đường dẫn tới thư mục chứa file .pickle
pickle_folder_path = "D:/archive/Cleaned Analyses/Cleaned Analyses"
topic_name = "toJson"  # Đặt tên cho topic của bạn

send_pickle_to_kafka(pickle_folder_path, topic_name)
