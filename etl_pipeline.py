import os
import sys
import logging
import datetime
from uuid import UUID
import azure.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, coalesce, round, avg, sum, count
from pyspark.sql.types import StringType

# Khởi tạo Azure Function App
app = func.FunctionApp()

# ==========================================
# 1. Đọc cấu hình từ Biến Môi Trường (Environment Variables)
# ==========================================
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "recruitment")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "tracking")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "etl_database")
MYSQL_TARGET_TABLE = os.environ.get("MYSQL_TARGET_TABLE", "events")

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


# ==========================================
# 2. Hàm Khởi tạo Spark Session
# ==========================================
def create_spark_session():
    """Khởi tạo SparkSession cục bộ cho lượt chạy hiện tại"""
    spark = SparkSession.builder \
        .appName("AzureFunction-ETL") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.cassandra.auth.username", CASSANDRA_USER) \
        .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==========================================
# 3. Các hàm Transform & Aggregation
# ==========================================


def process_df(df):
    @udf(returnType=StringType())
    def to_datetime_str(uuid_str):
        if not uuid_str:
            return None
        try:
            import uuid
            import datetime
            val = uuid.UUID(uuid_str)
            if val.version == 1:
                epoch = (val.time - 0x01b21dd213814000) / 10000000.0
                return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass
        return None

    if 'ts' in df.columns:
        df = df.drop('ts')
        
    return df.withColumn('ts', to_datetime_str(col('create_time')))


def calculating_clicks(spark, df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({
        'bid': 0.0, 'job_id': 0, 'publisher_id': 0, 'group_id': 0, 'campaign_id': 0
    })
    clicks_data.createOrReplaceTempView("clicks")
    
    return spark.sql("""
        SELECT 
            job_id, DATE(ts) AS date, HOUR(ts) AS hour, publisher_id, campaign_id, group_id, 
            ROUND(AVG(bid), 2) AS bid_set, COUNT(*) AS clicks, ROUND(SUM(bid), 2) AS spend_hour 
        FROM clicks
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)


def calculating_conversion(spark, df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({
        'job_id': 0, 'publisher_id': 0, 'group_id': 0, 'campaign_id': 0
    })
    conversion_data.createOrReplaceTempView("conversion")
    
    return spark.sql("""
        SELECT 
            job_id, DATE(ts) AS date, HOUR(ts) AS hour, publisher_id, campaign_id, group_id, 
            COUNT(*) AS conversions  
        FROM conversion
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)


def calculating_qualified(spark, df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({
        'job_id': 0, 'publisher_id': 0, 'group_id': 0, 'campaign_id': 0
    })
    qualified_data.createOrReplaceTempView("qualified")
    
    return spark.sql("""
        SELECT 
            job_id, DATE(ts) AS date, HOUR(ts) AS hour, publisher_id, campaign_id, group_id, 
            COUNT(*) AS qualified  
        FROM qualified
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)


def calculating_unqualified(spark, df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({
        'job_id': 0, 'publisher_id': 0, 'group_id': 0, 'campaign_id': 0
    })
    unqualified_data.createOrReplaceTempView("unqualified")
    
    return spark.sql("""
        SELECT 
            job_id, DATE(ts) AS date, HOUR(ts) AS hour, publisher_id, campaign_id, group_id, 
            COUNT(*) AS unqualified  
        FROM unqualified
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)


def process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output):
    join_keys = ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id']
    return clicks_output \
        .join(conversion_output, on=join_keys, how='full') \
        .join(qualified_output, on=join_keys, how='full') \
        .join(unqualified_output, on=join_keys, how='full')


def process_cassandra_data(spark, df):
    clicks_output = calculating_clicks(spark, df)
    conversion_output = calculating_conversion(spark, df)
    qualified_output = calculating_qualified(spark, df)
    unqualified_output = calculating_unqualified(spark, df)
    
    return process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output)


# ==========================================
# 4. Các hàm truy vấn database & nạp dữ liệu
# ==========================================
def retrieve_company_data(spark):
    sql = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) test"
    return spark.read.format('jdbc') \
        .options(url=MYSQL_URL, driver=MYSQL_DRIVER, dbtable=sql, user=MYSQL_USER, password=MYSQL_PASSWORD) \
        .load()


def import_to_mysql(output):
    final_output = output.select(
        'job_id', 'date', 'hour', 'publisher_id', 'company_id', 
        'campaign_id', 'group_id', 'unqualified', 'qualified', 
        'conversions', 'clicks', 'bid_set', 'spend_hour', 'updated_at'
    )
    final_output = final_output \
        .withColumnRenamed('date', 'dates') \
        .withColumnRenamed('hour', 'hours') \
        .withColumnRenamed('qualified', 'qualified_application') \
        .withColumnRenamed('unqualified', 'disqualified_application') \
        .withColumnRenamed('conversions', 'conversion')
    
    final_output = final_output.withColumn('sources', lit('Cassandra'))
    
    final_output.write.format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", MYSQL_URL) \
        .option("dbtable", MYSQL_TARGET_TABLE) \
        .mode("append") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .save()


def get_mysql_latest_time(spark):    
    sql = f"(SELECT MAX(updated_at) AS max_ts FROM {MYSQL_TARGET_TABLE}) data"
    try:
        mysql_time = spark.read.format('jdbc') \
            .options(url=MYSQL_URL, driver=MYSQL_DRIVER, dbtable=sql, user=MYSQL_USER, password=MYSQL_PASSWORD) \
            .load()
        mysql_time = mysql_time.take(1)[0][0]
    except Exception:
        mysql_time = None
        
    if mysql_time is None:
        return '1998-01-01 23:59:59'
    return mysql_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(mysql_time, 'strftime') else str(mysql_time)


# ==========================================
# 5. Hàm thực thi ETL chính (Main logic)
# ==========================================
def main_task(spark, mysql_time):
    # Đọc Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
        .load()
    
    # Lọc các dòng job_id isNotNull và lấy các cột cần thiết
    df = df.filter(df.job_id.isNotNull())
    df = df.select('create_time', 'job_id', 'custom_track', 'bid', 'campaign_id', 'group_id', 'publisher_id')
    
    # Thêm cột 'ts' bằng cách chạy process_df (chuyển đổi create_time sang timestamp dạng chuỗi)
    df = process_df(df)
    
    # Thực hiện lọc theo mốc thời gian đã đồng bộ gần nhất trên MySQL
    df = df.where(col('ts') >= mysql_time)
    
    # Check if empty trước khi thực hiện các bước tiếp theo để tiết kiệm tài nguyên
    if df.isEmpty():
        logging.info("Không phát hiện dữ liệu mới. Bỏ qua lượt chạy này.")
        return

    # Tính toán gộp các số liệu
    cassandra_output = process_cassandra_data(spark, df)
    
    # Lấy thông tin công ty từ MySQL và join bổ sung
    company = retrieve_company_data(spark)
    final_output = cassandra_output.join(company, 'job_id', 'left') \
        .drop(company.group_id) \
        .drop(company.campaign_id)
        
    # Tìm max ts từ dữ liệu thô Cassandra để gán vào cột updated_at
    from pyspark.sql.functions import to_timestamp
    max_ts = df.agg({'ts': 'max'}).take(1)[0][0]
    final_output = final_output.withColumn('updated_at', to_timestamp(lit(max_ts), 'yyyy-MM-dd HH:mm:ss'))
        
    # Ghi đè append vào MySQL
    import_to_mysql(final_output)


# ==========================================
# 6. Azure Function Entry Point (Timer Trigger)
# ==========================================
# Cấu hình cron chạy định kỳ mỗi 5 phút: "0 */5 * * * *"
@app.schedule(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def timer_trigger_etl(timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if timer.past_due:
        logging.warning('Timer bị trễ so với lịch trình!')

    logging.info(f'Function khởi động tại thời điểm UTC: {utc_timestamp}')
    
    # Khởi tạo Spark Session cho phiên làm việc này
    spark = create_spark_session()
    
    try:
        mysql_time = get_mysql_latest_time(spark)
        logging.info(f'Mốc thời gian đã đồng bộ gần nhất trên MySQL: {mysql_time}')
        
        logging.info(">>> Kích hoạt Main ETL Task...")
        main_task(spark, mysql_time)
            
    except Exception as e:
        logging.error(f"ETL Execution Failed: {str(e)}")
        
    finally:
        # Giải phóng Spark để giải phóng tài nguyên CPU/RAM của serverless container
        spark.stop()
        logging.info("Spark session đã dừng an toàn.")