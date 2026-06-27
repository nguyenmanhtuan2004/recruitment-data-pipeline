import os
import sys
import logging
import datetime
import json
import time
from uuid import UUID
import azure.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, coalesce, round, avg, sum, count
from pyspark.sql.types import StringType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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
        .config("spark.driver.memory", "512m") \
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
# Cấu hình cron chạy định kỳ mỗi 3 phút: "0 */3 * * * *"
@app.schedule(schedule="0 */3 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
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


# ==========================================
# 7. Giao diện Dashboard và APIs tương tác (Bất đồng bộ)
# ==========================================

# Endpoint phục vụ giao diện HTML Dashboard
@app.route(route="ui", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ui_dashboard(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Dashboard UI request received.")
    try:
        with open("/home/site/wwwroot/index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return func.HttpResponse(body=html_content, mimetype="text/html", status_code=200)
    except Exception as e:
        logging.error(f"Failed to read index.html: {str(e)}")
        return func.HttpResponse(body=f"Failed to load UI: {str(e)}", status_code=500)


# POST API: Tiếp nhận dữ liệu -> Lưu Cassandra -> Đẩy vào Queue -> Trả về 202 Accepted lập tức
@app.route(route="track", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.queue_output(arg_name="msg", queue_name="etl-trigger-queue", connection="AzureWebJobsStorage")
def track_event(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    import datetime
    import cassandra.util
    
    logging.info("API POST /api/track triggered (Asynchronous Ingestion).")
    try:
        # A. Đọc và phân giải Event Payload từ Request Body
        body = req.get_json()
        bid = int(float(body.get("bid", 0.0)))
        campaign_id = int(body.get("campaign_id", 0))
        custom_track = body.get("custom_track", "click")
        group_id = body.get("group_id")
        
        if group_id is not None:
            group_id = int(group_id)
            group_id_str = str(group_id)
        else:
            group_id_str = "null"
            
        job_id = int(body.get("job_id", 0))
        publisher_id = int(body.get("publisher_id", 0))
        
        # Tự động sinh ID thời gian và mốc thời gian dạng chuỗi
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.utcnow()))
        ts = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # B. Kết nối và ghi nhận vào Cassandra
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT), auth_provider=auth_provider)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        
        insert_sql = f"""
            INSERT INTO {CASSANDRA_TABLE} (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
            VALUES ('{create_time}', {bid}, {campaign_id}, '{custom_track}', {group_id_str}, {job_id}, {publisher_id}, '{ts}')
        """
        session.execute(insert_sql)
        cluster.shutdown()
        logging.info(f"Event ingested to Cassandra: {create_time}")
        
        # C. Đẩy tin nhắn vào Queue để kích hoạt ETL chạy ngầm
        queue_payload = {
            "trigger_type": "event_ingestion",
            "timestamp": ts,
            "latest_event_id": create_time
        }
        msg.set(json.dumps(queue_payload))
        logging.info("ETL trigger message pushed to Queue.")
        
        # D. Trả về phản hồi lập tức cho client (HTTP 202 Accepted)
        return func.HttpResponse(
            body=json.dumps({
                "status": "accepted",
                "message": "Event recorded. ETL pipeline scheduled in background.",
                "event": {
                    "create_time": create_time,
                    "ts": ts,
                    "custom_track": custom_track,
                    "job_id": job_id
                }
            }),
            mimetype="application/json",
            status_code=202
        )
        
    except Exception as e:
        logging.error(f"Event ingestion failed: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"status": "error", "message": f"Ingestion failed: {str(e)}"}),
            mimetype="application/json",
            status_code=400
        )


# Background Worker: Lắng nghe Queue -> Tự khởi chạy Spark ETL chạy ngầm dưới nền
@app.queue_trigger(arg_name="msg", queue_name="etl-trigger-queue", connection="AzureWebJobsStorage")
def queue_trigger_etl(msg: func.QueueMessage) -> None:
    try:
        payload = json.loads(msg.get_body().decode('utf-8'))
        logging.info(f"Background ETL Triggered by Queue. Message payload: {payload}")
    except Exception:
        logging.info("Background ETL Triggered by Queue.")
        
    # Khởi tạo Spark Session cho phiên làm việc nền này
    spark = create_spark_session()
    try:
        mysql_time = get_mysql_latest_time(spark)
        logging.info(f"Background Spark ETL starting from threshold: {mysql_time}")
        
        import time
        start_t = time.time()
        main_task(spark, mysql_time)
        duration = round(time.time() - start_t, 2)
        
        logging.info(f"Background Spark ETL completed successfully in {duration}s.")
    except Exception as e:
        logging.error(f"Background Spark ETL failed: {str(e)}")
    finally:
        spark.stop()
        logging.info("Background Spark session stopped safely.")
