import pickle
from kafka import KafkaConsumer
import json
import os
import logging

# Thiết lập logging để theo dõi lỗi và thông tin
logging.basicConfig(level=logging.INFO)

def consume_pickle_data(topic, kafka_server='localhost:9092', group_id='pickle-consumer-group'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: pickle.loads(x)
    )

    output_directory = r"D:\archive\Cleaned Analyses\toJson"  # Đường dẫn tới thư mục lưu tệp JSON

    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(output_directory, exist_ok=True)

    logging.info(f"Starting to consume data from topic {topic}...")
    
    for message in consumer:
        data = message.value
        json_file_path = os.path.join(output_directory, f'{message.offset}.json')  # Lưu tệp vào thư mục cụ thể
        
        try:
            with open(json_file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)
            logging.info(f'Data saved to {json_file_path}')
        except Exception as e:
            logging.error(f'Could not write JSON file {json_file_path}: {e}')

# Tên topic mà consumer sẽ lắng nghe
topic_name = "toJson"  # Đặt tên topic hợp lệ

consume_pickle_data(topic_name)
