import os
import sys
import time
import random
import json
import logging
import mysql.connector
import pandas as pd
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "etl_database")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "recruitment-tracking")

def get_job_data():
    cnx = mysql.connector.connect(
        user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE
    )
    df = pd.read_sql("SELECT id AS job_id, campaign_id, group_id FROM job", cnx)
    cnx.close()
    return df

def get_publisher_ids():
    cnx = mysql.connector.connect(
        user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE
    )
    df = pd.read_sql("SELECT id FROM master_publisher", cnx)
    cnx.close()
    return df['id'].tolist()

def main():
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    jobs = get_job_data()
    job_ids = jobs['job_id'].tolist()
    campaign_ids = jobs['campaign_id'].tolist()
    group_ids = jobs['group_id'].fillna(0).astype(int).tolist()
    publisher_ids = get_publisher_ids()
    
    interact = ['click', 'conversion', 'qualified', 'unqualified']
    
    logging.info("Bắt đầu sinh dữ liệu đẩy trực tiếp vào Kafka (Nhấn Ctrl+C để dừng)...")
    try:
        while True:
            custom_track = random.choices(interact, weights=(40, 25, 20, 15))[0]
            bid = random.choice([0, 1, 2, 5]) if custom_track == 'click' else 0
            
            idx = random.randint(0, len(job_ids) - 1)
            job_id = job_ids[idx]
            campaign_id = campaign_ids[idx]
            group_id = group_ids[idx]
            publisher_id = random.choice(publisher_ids)
            
            import uuid
            from datetime import datetime
            create_time = str(uuid.uuid1())
            ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            
            payload = {
                "create_time": create_time,
                "bid": bid,
                "campaign_id": campaign_id,
                "custom_track": custom_track,
                "group_id": group_id if group_id > 0 else None,
                "job_id": job_id,
                "publisher_id": publisher_id,
                "ts": ts
            }
            
            producer.send(KAFKA_TOPIC, payload)
            logging.info(f"Đã sản xuất event vào Kafka: track={custom_track}, job_id={job_id}")
            time.sleep(random.randint(1, 5))
            
    except KeyboardInterrupt:
        logging.info("Đã dừng tiến trình.")

if __name__ == "__main__":
    main()