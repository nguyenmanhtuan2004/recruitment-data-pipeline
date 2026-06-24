import os
import sys
import time
import random
import logging
import mysql.connector
import pandas as pd
import requests

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 1. Đọc cấu hình từ Biến Môi Trường (Environment Variables)
# ==========================================
API_URL = os.environ.get("API_URL", "http://127.0.0.1:8082/api/track")

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
# 3. Hàm gửi dữ liệu qua API (API Data Pusher)
# ==========================================
def push_dummy_data_via_api(n_records, job_list, campaign_list, group_list, publisher_list):
    interact = ['click', 'conversion', 'qualified', 'unqualified']
    
    for _ in range(n_records):
        custom_track = random.choices(interact, weights=(40, 25, 20, 15))[0]
        bid = random.choice([0, 1, 2, 5]) if custom_track == 'click' else 0
        
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher_list)
        campaign_id = random.choice(campaign_list)
        
        # Xử lý trường hợp group_list trống
        group_id = random.choice(group_list) if group_list else None
        
        # Tạo payload JSON gửi tới API
        payload = {
            "custom_track": custom_track,
            "bid": bid,
            "job_id": int(job_id),
            "publisher_id": int(publisher_id),
            "campaign_id": int(campaign_id),
            "group_id": int(group_id) if group_id is not None else None
        }
        
        try:
            logging.info(f"Đang bắn sự kiện tới API ({API_URL}): track={custom_track}, job_id={job_id}, bid={bid}")
            response = requests.post(API_URL, json=payload, headers={"Content-Type": "application/json"})
            
            if response.status_code == 202:
                logging.info(f"API phản hồi thành công (202): {response.json().get('message')}")
            else:
                logging.error(f"API phản hồi thất bại ({response.status_code}): {response.text}")
        except Exception as e:
            logging.error(f"Lỗi kết nối tới API: {str(e)}")


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

    logging.info(f"Cấu hình API Endpoint: {API_URL}")
    logging.info("Bắt đầu sinh dữ liệu tự động và bắn qua API (Nhấn Ctrl+C để dừng)...")
    
    try:
        while True:
            records_count = random.randint(1, 2)
            logging.info(f"Chuẩn bị tạo và bắn {records_count} bản ghi mới...")
            push_dummy_data_via_api(
                n_records=records_count,
                job_list=job_list,
                campaign_list=campaign_list,
                group_list=group_list,
                publisher_list=publisher_list
            )
            time.sleep(30)  # Tạo dữ liệu mỗi 30 giây
    except KeyboardInterrupt:
        logging.info("Đã dừng tiến trình sinh và bắn dữ liệu.")


if __name__ == "__main__":
    main()
