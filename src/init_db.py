import os
import sys
import time
import logging
import mysql.connector
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cấu hình MySQL
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "etl_database")

# Cấu hình Cassandra
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "recruitment")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "tracking")


def init_mysql():
    logging.info("=== Bắt đầu khởi tạo MySQL ===")
    
    # Kết nối MySQL (thử lại tối đa 5 lần)
    conn = None
    for attempt in range(1, 6):
        try:
            logging.info(f"Kết nối tới MySQL tại {MYSQL_HOST}:{MYSQL_PORT} (Lần thử {attempt}/5)...")
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )
            break
        except Exception as e:
            logging.warning(f"Chưa kết nối được MySQL: {str(e)}")
            if attempt == 5:
                logging.error("Lỗi: Không thể kết nối tới MySQL sau 5 lần thử.")
                sys.exit(1)
            time.sleep(5)

    cursor = conn.cursor()
    
    # 1. Tạo database nếu chưa tồn tại
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
    cursor.execute(f"USE {MYSQL_DATABASE}")
    logging.info(f"Đã đảm bảo database '{MYSQL_DATABASE}' tồn tại.")
    
    # 2. Tạo bảng master_publisher (Khớp với schema thực tế từ hình ảnh)
    # Kiểm tra cấu trúc cũ của master_publisher (nếu có)
    cursor.execute("SHOW TABLES LIKE 'master_publisher'")
    if cursor.fetchone():
        cursor.execute("DESCRIBE master_publisher")
        columns = cursor.fetchall()
        if len(columns) != 20:
            logging.info("Bảng 'master_publisher' cũ không khớp cấu trúc mới (20 cột). Đang tiến hành xóa và tạo lại...")
            cursor.execute("DROP TABLE master_publisher")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS master_publisher (
            id INT PRIMARY KEY,
            created_by VARCHAR(255),
            created_date TIMESTAMP NULL,
            last_modified_by VARCHAR(255),
            last_modified_date TIMESTAMP NULL,
            is_active TINYINT DEFAULT 1,
            publisher_name VARCHAR(255),
            publisher_email VARCHAR(255),
            access_token VARCHAR(255),
            publisher_type INT,
            publisher_group INT,
            publisher_code INT,
            publisher_currency VARCHAR(50),
            time_zone VARCHAR(100),
            cpc_increment DECIMAL(10,2) DEFAULT 0.00,
            bid_reading_interval INT DEFAULT 1,
            min_bid DECIMAL(10,2) DEFAULT 0.00,
            max_bid DECIMAL(10,2) DEFAULT 0.00,
            countries VARCHAR(255),
            data_sharing TEXT
        )
    """)
    logging.info("Đã tạo/kiểm tra bảng 'master_publisher' với đầy đủ cấu trúc thực tế.")

    # Kiểm tra cấu trúc cũ của job (nếu có)
    cursor.execute("SHOW TABLES LIKE 'job'")
    if cursor.fetchone():
        cursor.execute("DESCRIBE job")
        columns = cursor.fetchall()
        if len(columns) != 30:
            logging.info("Bảng 'job' cũ không khớp cấu trúc mới (30 cột). Đang tiến hành xóa và tạo lại...")
            cursor.execute("DROP TABLE job")

    # 3. Tạo bảng job (Đầy đủ 30 cột từ schema thực tế)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job (
            id INT PRIMARY KEY,
            created_by VARCHAR(255),
            created_date TIMESTAMP NULL,
            last_modified_by VARCHAR(255),
            last_modified_date TIMESTAMP NULL,
            is_active TINYINT DEFAULT 1,
            title VARCHAR(255),
            description TEXT,
            work_schedule VARCHAR(50),
            radius_unit VARCHAR(50),
            location_street VARCHAR(255),
            location_locality VARCHAR(255),
            role_location VARCHAR(50),
            resume_optional VARCHAR(50),
            budget DECIMAL(10,2),
            status INT,
            error VARCHAR(255),
            template_layout INT,
            template_options INT,
            question_template INT,
            redirect_url VARCHAR(255),
            start_date TIMESTAMP NULL,
            end_date TIMESTAMP NULL,
            close_date TIMESTAMP NULL,
            group_id INT,
            minor_id INT,
            campaign_id INT,
            company_id INT,
            history_store INT,
            ref_id INT
        )
    """)
    logging.info("Đã tạo/kiểm tra bảng 'job' với cấu trúc 30 cột.")

    # Kiểm tra cấu trúc cũ của events (nếu có) để xóa đi tạo lại nếu không khớp
    cursor.execute("SHOW TABLES LIKE 'events'")
    if cursor.fetchone():
        cursor.execute("DESCRIBE events")
        columns = cursor.fetchall()
        column_names = [col[0] for col in columns]
        if len(columns) != 17 or 'updated_at' not in column_names:
            logging.info("Bảng 'events' cũ không khớp cấu trúc mới (17 cột). Đang tiến hành xóa và tạo lại...")
            cursor.execute("DROP TABLE events")

    # 4. Tạo bảng events (Đích của ETL)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_id INT,
            dates DATE,
            hours INT,
            disqualified_application INT DEFAULT 0,
            qualified_application INT DEFAULT 0,
            conversion INT DEFAULT 0,
            company_id INT,
            group_id INT,
            campaign_id INT,
            publisher_id INT,
            bid_set DECIMAL(10,2) DEFAULT 0.00,
            clicks INT DEFAULT 0,
            impression INT DEFAULT 0,
            spend_hour DECIMAL(10,2) DEFAULT 0.00,
            sources VARCHAR(50) DEFAULT 'Cassandra',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    logging.info("Đã tạo/kiểm tra bảng 'events'.")

    # 5. Chèn dữ liệu mẫu vào master_publisher nếu bảng trống (Theo cấu trúc cột thực tế)
    cursor.execute("SELECT COUNT(*) FROM master_publisher")
    if cursor.fetchone()[0] == 0:
        insert_query = """
            INSERT INTO master_publisher (
                id, created_by, created_date, last_modified_by, last_modified_date, is_active, 
                publisher_name, publisher_email, access_token, publisher_type, publisher_group, publisher_code, 
                publisher_currency, time_zone, cpc_increment, bid_reading_interval, min_bid, max_bid, countries, data_sharing
            ) VALUES (%s, %s, NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        publishers = [
            (1, 'admin', 'admin', 1, 'Facebook', 'Facebook@mail.com', 'b01aba43-', 0, 1, 0, 1, 'SE Asia Standard Time', 0.00, 1, 1.00, 1.00, 'VN', '[]'),
            (2, 'admin', 'admin', 1, 'HiredCDC', 'HiredCDC@mail.com', '7106067c-', 1, 1, 0, 1, 'SE Asia Standard Time', 0.00, 1, 10.00, 10.00, 'VN', '[]'),
            (3, 'admin', 'admin', 1, 'Tatroo', 'Tatroo@mail.com', '7106067c-', 1, 1, 0, 1, 'SE Asia Standard Time', 0.00, 1, 10.00, 10.00, 'VN', '[]'),
            (4, 'admin', 'admin', 1, 'ZipperCrui', 'ZipperCrui@mail.com', '7106067c-', 1, 1, 0, 1, 'SE Asia Standard Time', 0.00, 1, 10.00, 10.00, 'VN', '[]'),
            (5, 'admin', 'admin', 1, 'Microsoft', 'Microsoft@mail.com', '7106067c-', 1, 1, 0, 1, 'SE Asia Standard Time', 0.00, 1, 10.00, 10.00, 'VN', '[]'),
            (9, 'admin', 'admin', 0, 'textUrl 12', 'letheanh2000@mail.com', 'd8fbc72b-', 0, 0, 1, 1, '(UTC-12:00)', 0.00, 0, 12.00, 23.00, 'Anguilla', '[]'),
            (10, 'admin', 'admin', 0, 'le the anh', 'letheanh2000@mail.com', '652440c8-', 0, 0, 2, 1, '(UTC-10:00)', 0.00, 0, 1.00, 2.00, 'Angola,Botswana', '[{"id":1,"companyName":"Gotoro"}]'),
            (11, 'admin', 'admin', 0, 'le the anh', 'letheanh2000@mail.com', '70fc8b59-', 0, 0, 0, 1, '(UTC-08:00)', 0.00, 0, 1.00, 122.00, 'Afghanistan', '[{"id":1,"companyName":"Gotoro"}]'),
            (12, 'admin', 'admin', 0, 'FeedName 4', 'letheanh2000@mail.com', 'c33eb178-', 1, 1, 5, 1, '-12', 0.00, 0, 1.00, 2.00, 'Afghanistan', '[{"id":1,"companyName":"Gotoro"}]'),
            (13, 'admin', 'admin', 0, 'Microsoft', 'letheanh2000@mail.com', '18f21d58-', 0, 0, 0, 1, '(UTC-08:00)', 1.00, 0, 1.00, 2.00, 'Angola', '[{"id":1,"companyName":"Gotoro"}]'),
            (15, 'admin', 'admin', 0, 'Microsoft', 'letheanh2000@mail.com', 'bfcd974c-', 0, 1, 6, 1, '(UTC-11:00)', 2.00, 1, 1.00, 2.00, 'Antarctica', '[{"id":1,"companyName":"Gotoro"}]'),
            (16, 'admin', 'admin', 0, 'Microsoft', 'letheanh2000@mail.com', '1402bba1-', 0, 0, 0, 1, '(UTC-12:00)', 1.00, 0, 1.00, 2.00, 'American Samoa', '[{"id":1,"companyName":"Gotoro"}]'),
            (17, 'admin', 'admin', 1, '1', 'pmt58200@mail.com', '79589776-', 0, 1, 0, 1, '(UTC-07:00)', 1.00, 1, 1.00, 1.00, 'Angola,Anguilla', '[{"id":1,"companyName":"Gotoro"}]'),
            (20, 'admin', 'admin', 1, 'FeedName', 'pmt58200@mail.com', '529f1d50-', 0, 1, 0, 1, '(UTC-07:00)', 0.00, 1, 1.00, 1.00, 'Antigua and Barbuda', '[{"id":1,"companyName":"Gotoro"}]'),
            (22, 'admin', 'admin', 1, 'FeedName', 'letheanh2000@mail.com', '09e63575-', 1, 1, 0, 1, '(UTC-08:00)', 1.00, 1, 1.00, 1222.00, 'Algeria', '[{"id":1,"companyName":"Gotoro"},{"id":13,"companyName":"Gotoro 67"}]'),
            (23, 'admin', 'admin', 0, 'le the anh', 'letheanh2000@mail.com', 'd07dc176-', 0, 1, 1, 2, '(UTC-10:00)', 0.00, 0, 1.00, 100.00, 'Falkland Islands', '[{"id":10,"companyName":"Gotoro 6"}]'),
            (24, 'admin', 'admin', 1, '1231', '123@mail.com', '123', 0, 1, 0, 1, '(UTC-09:00)', 1.00, 0, 123.00, 123.00, 'Angola,Antigua and Barbuda', '[{"id":15,"companyName":"Gotoro 10052"}]'),
            (27, 'admin', 'admin', 1, 'bao test', '123@gmai.com', '340c594b-', 0, 0, 0, 1, '(UTC-07:00)', 1.00, 1, 1.00, 1.00, 'VN', '[{"id":13,"companyName":"Gotoro 67"}]'),
            (28, 'admin', 'admin', 0, '123', 'safdasfasf@mail.com', '5a4fd94f-', 0, 0, 1, 1, '(UTC-08:00)', 0.00, 1, 1.00, 1.00, 'Andorra', '[]'),
            (29, 'admin', 'admin', 0, 'tesst4', 'tesst4@gr.com', '1f373256-', 0, 0, 5, 1, '(UTC-08:00)', 0.00, 0, 1.00, 1.00, 'Albania', '[]'),
            (30, 'admin', 'admin', 0, 'tesst5', 'tesst5@gr.com', 'bedd77f2-', 0, 0, 4, 1, '(UTC-10:00)', 1.00, 0, 12.00, 12.00, 'Falkland Islands', '[{"id":13,"companyName":"Gotoro 67"}]'),
            (31, 'admin', 'admin', 0, 'tesst6', 'tesst6.gm@mail.com', '653945e2-', 0, 0, 4, 1, '(UTC-07:00)', 0.00, 0, 1.00, 1.00, 'Angola', '[{"id":15,"companyName":"Gotoro 10052"}]'),
            (32, 'admin', 'admin', 1, 'le the anh', 'letheanh2000@mail.com', '18de19ad-', 1, 1, 0, 2, '(UTC-11:00)', 1.00, 1, 1200.00, 122221.00, 'Algeria', '[{"id":33,"companyName":"managedclient23"}]'),
            (33, 'admin', 'admin', 0, 'le the anh', 'letheanh2000@mail.com', 'aefbd7be-', 1, 0, 5, 2, '(UTC-08:00)', 2.00, 0, 12.00, 1234.00, 'Algeria,Angola', '[{"id":25,"companyName":"test user 1000"},{"id":39,"companyName":"letheanh test 2305 1102"}]'),
            (34, 'admin', 'admin', 1, 'le the anh', 'letheanh2000@mail.com', '92e32b7a-', 1, 0, 0, 2, '(UTC-12:00)', 1.00, 0, 1.00, 1200.00, 'Albania', '[{"id":13,"companyName":"Gotoro 67"}]'),
            (35, 'admin', 'admin', 0, 'test maste', 'testmaste@mail.com', '3bb835e8-', 0, 1, 0, 2, '(UTC-11:00)', 1.00, 0, 1.00, 1200.00, 'Andorra,Albania', '[{"id":3,"companyName":"Gotoro 0805"},{"id":13,"companyName":"Gotoro 67"}]'),
            (36, 'admin', 'admin', 0, 'test create', 'letheanh06070857@mail.com', '06070857-', 0, 1, 0, 2, '(UTC-12:00)', 0.00, 0, 1.00, 1200.00, 'Falkland Islands', '[{"id":8,"companyName":"Publisher Company"},{"id":15,"companyName":"Gotoro 10052"}]'),
            (37, 'admin', 'admin', 0, 'Publisher 1', 'letheanh08071044@mail.com', '08071044-', 0, 1, 0, 1, '(UTC-11:00)', 0.00, 0, 123.00, 1234.00, 'American Samoa', '[{"id":15,"companyName":"Gotoro 10052"}]'),
            (38, 'admin', 'admin', 0, 'Publisher 2', 'letheanh08071044@mail.com', '08071044-', 0, 1, 0, 1, '(UTC-08:00)', 1.00, 0, 1.00, 123.00, 'Andorra', '[{"id":10,"companyName":"Gotoro 6"}]')
        ]
        cursor.executemany(insert_query, publishers)
        conn.commit()
        logging.info(f"Đã chèn {len(publishers)} nhà phát hành mẫu vào bảng 'master_publisher'.")
    else:
        logging.info("Bảng 'master_publisher' đã có dữ liệu. Bỏ qua chèn seed data.")

    # 6. Chèn dữ liệu mẫu vào bảng job nếu bảng trống (Với đầy đủ 30 cột và các Jobs có trong events)
    cursor.execute("SELECT COUNT(*) FROM job")
    if cursor.fetchone()[0] == 0:
        insert_job_query = """
            INSERT INTO job (
                id, created_by, created_date, last_modified_by, last_modified_date, is_active,
                title, description, work_schedule, radius_unit, location_street, location_locality,
                role_location, resume_optional, budget, status, error, template_layout,
                template_options, question_template, redirect_url, start_date, end_date, close_date,
                group_id, minor_id, campaign_id, company_id, history_store, ref_id
            ) VALUES (
                %s, %s, NOW(), %s, NOW(), %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """
        jobs = [
            # Job 1
            (1, 'admin', 'myuser', 1, 
             'Python Developer', 'Description Python Developer', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1', None, None, None,
             20, 1, 10, 1, 2, 2),
            # Job 2 (Khớp với screenshot người dùng cung cấp)
            (2, 'admin', 'myuser', 1, 
             'PHP Developer', 'Description PHP Developer', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/2', None, None, None,
             10, 1, 1, 1, 2, 2),
            # Job 3 (Khớp với screenshot)
            (3, 'admin', 'admin', 0, 
             'Java Developer', 'Description Java Developer', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             17, None, 'http://localhost/job/3', None, None, None,
             10, 1, 1, 1, 2, 4),
            # Job 4 (Khớp với screenshot)
            (4, 'admin', 'admin', 0, 
             'C# Developer', 'Description C# Developer', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             18, None, 'http://localhost/job/4', None, None, None,
             10, 1, 1, 1, 2, 5),
            # Job 5 (Khớp với screenshot)
            (5, 'admin', 'admin', 0, 
             'NodeJS Developer', 'Description NodeJS Developer', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             19, None, 'http://localhost/job/5', None, None, None,
             10, 1, 1, 1, 2, 6),
            # Job 6 (Khớp với screenshot)
            (6, 'admin', 'admin', 0, 
             'Photoshop Designer', 'Description Photoshop Designer', 'FULL_TIME', None, None, None,
             'REMOTE', 'Optional', None, 2, None, None,
             20, None, 'http://localhost/job/6', None, None, None,
             10, 1, 1, 1, 2, 7),
            # Job 7 (Khớp với screenshot)
            (7, 'admin', 'admin', 0, 
             'Figma Tester', 'Description Figma Tester', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             21, None, 'http://localhost/job/7', None, None, None,
             10, 1, 1, 1, 2, 8),
            # Job 8 (Khớp với screenshot)
            (8, 'admin', 'admin', 0, 
             'IS Team', 'Description IS Team', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             22, None, 'http://localhost/job/8', None, None, None,
             10, 1, 1, 1, 2, 9),
            # Job 9 (Khớp với screenshot)
            (9, 'admin', 'admin', 0, 
             'Design Team', 'Description Design Team', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', None, 2, None, None,
             23, None, 'http://localhost/job/9', None, None, None,
             10, 1, 1, 1, 2, 10),
            # Job 39 (Từ 123/123123 pattern)
            (39, 'admin', 'admin', 0, 
             '123', '123123', 'Full Time', None, None, None,
             None, 'Required', None, 2, None, None,
             123123, None, 'http://localhost/job/39', None, None, None,
             10, 1, 1, 1, 2, 39),
            # Job 89 (Từ Part Time pattern)
            (89, 'admin', 'admin', 0, 
             '123', '123', 'Part Time', None, None, None,
             None, 'Required', None, 1, None, None,
             123123, None, 'http://localhost/job/89', None, None, None,
             None, 1, 5, 1, 1, 89),
            # Job 96 (Từ Job1 pattern)
            (96, 'admin', 'admin', 0, 
             'Job1', 'Desc', 'Contract', None, None, None,
             'Remote', 'Required', None, 3, None, None,
             16, None, 'https://translate.google.com/?hl=vi&sl=en&tl=vi&op=t', None, None, None,
             None, 1, 4, 1, 1, 96),
            # Job 98 (Khớp hoàn hảo với events và screenshot)
            (98, 'admin', 'self_client', 1, 
             'Job1', 'Desc', 'Contract', None, None, None,
             'Remote', 'Required', None, 1, None, None,
             16, None, 'https://translate.google.com/?hl=vi&sl=en&tl=vi&op=t', None, None, None,
             None, 1, 4, 1, 2, 98),
            # Job 114 (Sales Exec)
            (114, 'admin', 'myuser', 1, 
             'Sales Exec', 'Do you', 'Full Time', None, None, None,
             'REMOTE', 'OPTIONAL', None, 2, '["Benefit is missing.", "Lead is missing."]', None,
             16, None, 'http://localhost/job/114', None, None, None,
             10, 1, 1, 1, 2, 114),
            # Job 116 (job 132 pattern)
            (116, 'admin', 'admin', 1, 
             'job 132', 'Desc', 'Full Time', None, None, None,
             'Remote', 'Required', None, 1, None, None,
             16, None, 'https://translate.google.com/?hl=vi&sl=en&tl=vi&op=t', None, None, None,
             10, 1, 1, 1, 2, 116),
            # Job 132 (test job Part Time)
            (132, 'admin', 'admin', 0, 
             'test job', 'test job', 'Part Time', None, None, None,
             None, 'Required', None, 3, None, None,
             16, None, 'https://www.topcv.vn/tim-viec-lam-tester-tai-da-nang', None, None, None,
             None, 1, 33, 1, 1, 132),
            # Job 139 (test so luo Part Time)
            (139, 'admin', 'admin', 0, 
             'test so luo', '22 Part Time', 'Part Time', None, None, None,
             None, 'Required', None, 3, None, None,
             16, None, 'https://www.topcv.vn/tim-viec-lam-tester-tai-da-nang', None, None, None,
             None, 2, 33, 1, 1, 139),
            # Job 142 (self_client tester)
            (142, 'self_client', 'admin', 1, 
             'tester', 'tt', 'Part Time', None, None, None,
             None, 'Required', None, 3, None, None,
             16, None, 'https://www.topcv.vn/tim-viec-lam-tester-tai-da-nang', None, None, None,
             None, 1, 64, 33, 3, 142),
            # Job 187 (Từ dữ liệu mẫu events)
            (187, 'admin', 'myuser', 1, 
             'Java Developer', 'Description Java Dev', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/187', None, None, None,
             None, 1, 48, 33, 2, 2),
            # Job 188 (Từ dữ liệu mẫu events)
            (188, 'admin', 'myuser', 1, 
             'Golang Developer', 'Description Golang Dev', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/188', None, None, None,
             None, 1, 48, 33, 2, 2),
            # Job 258 (Từ dữ liệu mẫu events)
            (258, 'admin', 'myuser', 1, 
             'C++ Developer', 'Description C++ Dev', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/258', None, None, None,
             None, 1, 93, 33, 2, 2),
            # Job 273 (Từ dữ liệu mẫu events)
            (273, 'admin', 'myuser', 1, 
             'Ruby Developer', 'Description Ruby Dev', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/273', None, None, None,
             None, 1, 48, 33, 2, 2),
            # Job 1527 (Từ dữ liệu mẫu events)
            (1527, 'admin', 'myuser', 1, 
             'DevOps Engineer', 'Description DevOps', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1527', None, None, None,
             10, 1, 1, 1, 2, 2),
            # Job 1529 (Từ dữ liệu mẫu events)
            (1529, 'admin', 'myuser', 1, 
             'Security Engineer', 'Description Security', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1529', None, None, None,
             10, 1, 1, 1, 2, 2),
            # Job 1530 (Từ dữ liệu mẫu events)
            (1530, 'admin', 'myuser', 1, 
             'QA Engineer', 'Description QA', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1530', None, None, None,
             10, 1, 1, 1, 2, 2),
            # Job 1531 (Từ dữ liệu mẫu events)
            (1531, 'admin', 'myuser', 1, 
             'AI Engineer', 'Description AI', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1531', None, None, None,
             30, 1, 53, 40, 2, 2),
            # Job 1532 (Từ dữ liệu mẫu events)
            (1532, 'admin', 'myuser', 1, 
             'Cloud Architect', 'Description Cloud', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1532', None, None, None,
             31, 1, 69, 15, 2, 2),
            # Job 1533 (Từ dữ liệu mẫu events)
            (1533, 'admin', 'myuser', 1, 
             'Mobile Developer', 'Description Mobile', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1533', None, None, None,
             31, 1, 69, 15, 2, 2),
            # Job 1534 (Từ dữ liệu mẫu events)
            (1534, 'admin', 'myuser', 1, 
             'Product Owner', 'Description PO', 'FULL_TIME', None, None, None,
             'REMOTE', 'REQUIRED', 50.00, 1, None, None,
             16, None, 'http://localhost/job/1534', None, None, None,
             30, 1, 53, 40, 2, 2)
        ]
        cursor.executemany(insert_job_query, jobs)
        conn.commit()
        logging.info(f"Đã chèn {len(jobs)} tin tuyển dụng mẫu vào bảng 'job'.")
    else:
        logging.info("Bảng 'job' đã có dữ liệu. Bỏ qua chèn seed data.")

    # 7. Chèn dữ liệu mẫu vào bảng events nếu bảng trống (Từ các ảnh chụp màn hình)
    cursor.execute("SELECT COUNT(*) FROM events")
    if cursor.fetchone()[0] == 0:
        insert_events_query = """
            INSERT INTO events (
                id, job_id, dates, hours, disqualified_application, qualified_application, 
                conversion, company_id, group_id, campaign_id, publisher_id, bid_set, 
                clicks, impression, spend_hour, sources
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        events = [
            (2089, 98, '2022-07-08', 9, 1, 0, 1, 1, None, 4, 1, 2.00, 108, None, 216.00, 'Cassandra'),
            (2090, 98, '2022-07-13', 15, 1, 0, 1, 1, None, 4, 1, 2.00, 1, None, 2.00, 'Cassandra'),
            (2091, 187, '2022-07-08', 4, 1, 0, 1, 33, None, 48, 1, 0.00, 2, None, 0.00, 'Cassandra'),
            (2092, 187, '2022-07-08', 6, 1, 0, 1, 33, None, 48, 1, 1.50, 6, None, 9.00, 'Cassandra'),
            (2093, 258, '2022-07-08', 9, 1, 0, 1, 33, None, 93, 3, 2.00, 1, None, 2.00, 'Cassandra'),
            (2094, 2, '2022-07-14', 9, 3, 0, 3, 1, 10, 1, 1, None, None, None, None, 'Cassandra'),
            (2101, 188, '2022-07-24', 10, 1, 0, 1, 33, None, 48, 1, 1.00, 25, None, 25.00, 'Cassandra'),
            (2102, 188, '2022-07-24', 14, 1, 0, 1, 33, None, 48, 1, 1.00, 86, None, 86.00, 'Cassandra'),
            (2115, 188, '2022-07-25', 9, None, None, None, 33, None, 48, 1, 1.00, 52, None, 52.00, 'Cassandra'),
            (2116, 273, '2022-07-25', 10, None, None, None, 33, None, 48, 1, 2.00, 8, None, 0.00, 'Cassandra'),
            (2117, 1527, '2022-07-25', 9, None, None, None, 1, 10, 1, 1, 1.00, 3, None, 3.00, 'Cassandra'),
            (2118, 1527, '2022-07-25', 10, None, None, None, 1, 10, 1, 1, 1.00, 13, None, 13.00, 'Cassandra'),
            (2119, 1529, '2022-07-25', 9, None, None, None, 1, 10, 1, 1, 1.50, 3, None, 4.50, 'Cassandra'),
            (2120, 1529, '2022-07-25', 10, None, None, None, 1, 10, 1, 1, 1.50, 10, None, 15.00, 'Cassandra'),
            (2121, 1530, '2022-07-25', 9, None, None, None, 1, 10, 1, 1, 2.00, 11, None, 22.00, 'Cassandra'),
            (2122, 1530, '2022-07-25', 10, None, None, None, 1, 10, 1, 1, 2.00, 8, None, 16.00, 'Cassandra'),
            (2123, 1531, '2022-07-25', 9, None, None, None, 40, 30, 53, 1, 2.00, 23, None, 46.00, 'Cassandra'),
            (2124, 1532, '2022-07-25', 9, None, None, None, 15, 31, 69, 1, 1.00, 17, None, 17.00, 'Cassandra'),
            (2125, 1533, '2022-07-25', 9, None, None, None, 15, 31, 69, 1, 1.00, 19, None, 19.00, 'Cassandra'),
            (2126, 1534, '2022-07-25', 9, None, None, None, 40, 30, 53, 1, 1.00, 17, None, 17.00, 'Cassandra')
        ]
        cursor.executemany(insert_events_query, events)
        conn.commit()
        logging.info(f"Đã chèn {len(events)} dòng dữ liệu mẫu vào bảng 'events'.")
    else:
        logging.info("Bảng 'events' đã có dữ liệu. Bỏ qua chèn seed data.")

    cursor.close()
    conn.close()
    logging.info("=== Khởi tạo MySQL thành công ===\n")


def init_cassandra():
    logging.info("=== Bắt đầu khởi tạo Cassandra ===")
    
    # Kết nối Cassandra (thử lại tối đa 5 lần)
    cluster = None
    session = None
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    
    for attempt in range(1, 6):
        try:
            logging.info(f"Kết nối tới Cassandra tại {CASSANDRA_HOST}:{CASSANDRA_PORT} (Lần thử {attempt}/5)...")
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
            session = cluster.connect()
            break
        except Exception as e:
            logging.warning(f"Chưa kết nối được Cassandra: {str(e)}")
            if attempt == 5:
                logging.error("Lỗi: Không thể kết nối tới Cassandra sau 5 lần thử.")
                sys.exit(1)
            time.sleep(5)

    # 1. Tạo Keyspace nếu chưa tồn tại
    create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': '1'
        }}
    """
    session.execute(create_keyspace_query)
    logging.info(f"Đã đảm bảo keyspace '{CASSANDRA_KEYSPACE}' tồn tại.")
    
    # Thiết lập sử dụng keyspace
    session.set_keyspace(CASSANDRA_KEYSPACE)
    
    # 2. Tạo Table tracking nếu chưa tồn tại
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
            create_time text PRIMARY KEY,
            bid int,
            bn text,
            campaign_id int,
            cd int,
            custom_track text,
            de text,
            dl text,
            dt text,
            ed text,
            ev int,
            group_id int,
            id text,
            job_id int,
            md text,
            publisher_id int,
            rl text,
            sr text,
            ts text,
            tz int,
            ua text,
            uid text,
            utm_campaign text,
            utm_content text,
            utm_medium text,
            utm_source text,
            utm_term text,
            v int,
            vp text
        )
    """
    session.execute(create_table_query)
    logging.info(f"Đã đảm bảo bảng '{CASSANDRA_TABLE}' tồn tại trong Cassandra.")
    
    cluster.shutdown()
    logging.info("=== Khởi tạo Cassandra thành công ===\n")


def main():
    init_mysql()
    init_cassandra()
    logging.info("🎉 HỆ THỐNG CƠ SỞ DỮ LIỆU ĐÃ SẴN SÀNG HOẠT ĐỘNG!")


if __name__ == "__main__":
    main()
