import threading
import subprocess
from kafka import KafkaConsumer
from dotenv import load_dotenv
import gzip
import pickle
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
def run_producer():
    subprocess.run(["python", "producer.py"])

def run_consumer():
    subprocess.run(["python", "consumer.py"])

if __name__ == "__main__":
    # Tạo các thread cho producer và consumer
    producer_thread = threading.Thread(target=run_producer)
    consumer_thread = threading.Thread(target=run_consumer)

    # Bắt đầu các thread
    producer_thread.start()
    consumer_thread.start()

    # Đợi các thread hoàn thành
    producer_thread.join()
    consumer_thread.join()
