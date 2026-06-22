import os
import sys
import time
import random
import datetime
import logging
import mysql.connector
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra.util

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 1. Đọc cấu hình từ Biến Môi Trường (Environment Variables)
# ==========================================
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "recruitment")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "tracking")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "etl_database")


# ==========================================
# 2. Các hàm kết nối & truy vấn MySQL
# ==========================================
def get_data_from_job():
    logging.info(f"Đang kết nối MySQL tại {MYSQL_HOST}:{MYSQL_PORT} để lấy dữ liệu Job...")
    try:
        cnx = mysql.connector.connect(
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE
        )
        query = "SELECT id AS job_id, campaign_id, group_id, company_id FROM job"
        mysql_data = pd.read_sql(query, cnx)
        cnx.close()
        return mysql_data
    except Exception as e:
        logging.error(f"Lỗi khi kết nối MySQL (lấy dữ liệu Job): {str(e)}")
        sys.exit(1)


def get_data_from_publisher():
    logging.info(f"Đang kết nối MySQL tại {MYSQL_HOST}:{MYSQL_PORT} để lấy dữ liệu Publisher...")
    try:
        cnx = mysql.connector.connect(
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE
        )
        query = "SELECT DISTINCT id AS publisher_id FROM master_publisher"
        mysql_data = pd.read_sql(query, cnx)
        cnx.close()
        return mysql_data
    except Exception as e:
        logging.error(f"Lỗi khi kết nối MySQL (lấy dữ liệu Publisher): {str(e)}")
        sys.exit(1)


# ==========================================
# 3. Hàm tạo dữ liệu ảo (Dummy Data Generator)
# ==========================================
def generating_dummy_data(n_records, session, job_list, campaign_list, group_list, publisher_list):
    interact = ['click', 'conversion', 'qualified', 'unqualified']
    
    for _ in range(n_records):
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.utcnow()))
        bid = random.choice([0.0, 0.05, 0.1, 0.2, 0.5, 1.0])  # Giá bid ngẫu nhiên
        custom_track = random.choices(interact, weights=(70, 10, 10, 10))[0]
        
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher_list)
        campaign_id = random.choice(campaign_list)
        
        # Xử lý trường hợp group_list trống
        group_id = random.choice(group_list) if group_list else "null"
        
        # ts format UTC
        ts = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # Query CQL - Không đóng dấu nháy đơn cho các trường số hoặc uuid trong Cassandra
        sql = f"""
            INSERT INTO {CASSANDRA_TABLE} (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
            VALUES ({create_time}, {bid}, {campaign_id}, '{custom_track}', {group_id}, {job_id}, {publisher_id}, '{ts}')
        """
        try:
            session.execute(sql)
            logging.info(f"Đã insert: job_id={job_id}, track={custom_track}, bid={bid}, ts={ts}")
        except Exception as e:
            logging.error(f"Lỗi khi insert dữ liệu vào Cassandra: {str(e)}")


# ==========================================
# 4. Luồng chạy chính (Main Loop)
# ==========================================
def main():
    # 1. Đọc dữ liệu từ MySQL trước
    jobs_data = get_data_from_job()
    publisher_data = get_data_from_publisher()
    
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    publisher_list = publisher_data['publisher_id'].to_list()
    
    # Lọc danh sách group_id hợp lệ
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    
    if not job_list or not publisher_list:
        logging.error("Không có dữ liệu trong bảng job hoặc master_publisher của MySQL. Hãy điền dữ liệu trước.")
        sys.exit(1)

    # 2. Kết nối Cassandra
    logging.info(f"Đang kết nối Cassandra tại {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    try:
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logging.info(f"Đã kết nối thành công Keyspace: '{CASSANDRA_KEYSPACE}'")
    except Exception as e:
        logging.error(f"Không thể kết nối Cassandra: {str(e)}")
        sys.exit(1)

    # 3. Vòng lặp tạo dữ liệu liên tục
    logging.info("Bắt đầu sinh dữ liệu tự động (Nhấn Ctrl+C để dừng)...")
    try:
        while True:
            records_count = random.randint(1, 10)
            logging.info(f"Chuẩn bị tạo {records_count} bản ghi mới...")
            generating_dummy_data(
                n_records=records_count,
                session=session,
                job_list=job_list,
                campaign_list=campaign_list,
                group_list=group_list,
                publisher_list=publisher_list
            )
            time.sleep(10)  # Tạo dữ liệu mỗi 10 giây để kiểm thử real-time
    except KeyboardInterrupt:
        logging.info("Đã dừng tiến trình sinh dữ liệu.")
    finally:
        cluster.shutdown()
        logging.info("Đã đóng kết nối Cassandra.")


if __name__ == "__main__":
    main()
