import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Đọc cấu hình từ biến môi trường
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "recruitment-tracking")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "recruitment")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "tracking")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "123")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "etl_database")
MYSQL_TARGET_TABLE = os.environ.get("MYSQL_TARGET_TABLE", "events")

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?rewriteBatchedStatements=true&useSSL=false"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# Khởi tạo Spark Session với đầy đủ packages kết nối
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming-Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.cassandra.auth.username", CASSANDRA_USER) \
    .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Định nghĩa Schema của dữ liệu JSON nhận từ Kafka
event_schema = StructType([
    StructField("create_time", StringType(), True),
    StructField("bid", IntegerType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("custom_track", StringType(), True),
    StructField("group_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True),
    StructField("publisher_id", IntegerType(), True),
    StructField("ts", StringType(), True)
])

# Hàm lấy metadata Job từ MySQL (để join lấy company_id)
def retrieve_job_metadata(spark_session):
    sql = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) job_meta"
    return spark_session.read.format('jdbc') \
        .options(url=MYSQL_URL, driver=MYSQL_DRIVER, dbtable=sql, user=MYSQL_USER, password=MYSQL_PASSWORD) \
        .load()

# --- Cơ chế Cache & Auto-refresh Metadata Job định kỳ từ MySQL ---
job_metadata_df = None
last_metadata_refresh_time = 0.0

def get_job_metadata(spark_session):
    global last_metadata_refresh_time, job_metadata_df
    current_time = time.time()
    
    # Khởi tạo lần đầu (Lazy Loading) khi có micro-batch đầu tiên đổ về
    if job_metadata_df is None:
        logging.info(">>> Khởi tạo lần đầu: Tiến hành tải và Cache metadata Job từ MySQL...")
        job_metadata_df = retrieve_job_metadata(spark_session).cache()
        last_metadata_refresh_time = current_time
    # Tự động reload sau mỗi 5 phút (300 giây) để nạp Jobs mới từ MySQL
    elif current_time - last_metadata_refresh_time > 300:
        logging.info(">>> Định kỳ 5 phút: Tiến hành làm mới (reload) metadata Job từ MySQL...")
        try:
            job_metadata_df.unpersist()
            job_metadata_df = retrieve_job_metadata(spark_session).cache()
            last_metadata_refresh_time = current_time
            logging.info(">>> Làm mới metadata Job thành công.")
        except Exception as e:
            logging.error(f"Lỗi khi reload metadata Job: {str(e)}. Vẫn tiếp tục sử dụng cache cũ.")
            
    return job_metadata_df

def execute_with_retry(write_func, target_name="Database", max_retries=3, initial_delay=2):
    for attempt in range(1, max_retries + 1):
        try:
            write_func()
            return
        except Exception as e:
            if attempt == max_retries:
                logging.error(f"[Retry Failure] Đã thử {max_retries} lần nhưng gi {target_name} vẫn thất bại! Lỗi: {e}")
                raise e

            wait_seconds = initial_delay * (2** (attempt - 1))
            logging.warning(
                f"[Retry Attempt {attempt}/{max_retries}] Lỗi ghi {target_name} ({e})."
                f"Đang thử lại sau {wait_seconds} giây..."
            )
            time.sleep(wait_seconds)

# Hàm xử lý logic cho từng micro-batch nhận từ Kafka
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    start_time = time.time()
    logging.info(f"=== Đang xử lý Micro-batch {batch_id} - Số lượng bản ghi: {batch_df.count()} ===")
    
    # Lấy SparkSession riêng của batch DataFrame (bắt buộc cho Structured Streaming)
    batch_spark = batch_df.sparkSession

    # 1. Ghi nhận dữ liệu thô vào Cassandra (Data Lake)
    t_start = time.time()
    raw_to_save = batch_df.select(
        'create_time', 'bid', 'campaign_id', 'custom_track', 'group_id', 'job_id', 'publisher_id', 'ts'
    )
    def save_to_cassandra():
        raw_to_save.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode("append") \
            .save()
    execute_with_retry(save_to_cassandra, target_name="Cassandra", max_retries=3, initial_delay=2)
    logging.info(f"Đã ghi log thô thành công vào Cassandra. (Thời gian: {time.time() - t_start:.3f}s)")

    # 2. Xử lý tổng hợp (Aggregation) & 3. Join với Metadata
    t_start = time.time()
    # Chia nhánh tính toán (đăng ký view trên batch_spark)
    batch_df.createOrReplaceTempView("raw_events")
    
    clicks_df = batch_spark.sql("""
        SELECT job_id, DATE(ts) AS dates, HOUR(ts) AS hours, publisher_id, campaign_id, group_id,
               ROUND(AVG(bid), 2) AS bid_set, COUNT(*) AS clicks, ROUND(SUM(bid), 2) AS spend_hour
        FROM raw_events WHERE custom_track = 'click'
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)
    
    conversions_df = batch_spark.sql("""
        SELECT job_id, DATE(ts) AS dates, HOUR(ts) AS hours, publisher_id, campaign_id, group_id,
               COUNT(*) AS conversion
        FROM raw_events WHERE custom_track = 'conversion'
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)
    
    qualified_df = batch_spark.sql("""
        SELECT job_id, DATE(ts) AS dates, HOUR(ts) AS hours, publisher_id, campaign_id, group_id,
               COUNT(*) AS qualified_application
        FROM raw_events WHERE custom_track = 'qualified'
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)
    
    unqualified_df = batch_spark.sql("""
        SELECT job_id, DATE(ts) AS dates, HOUR(ts) AS hours, publisher_id, campaign_id, group_id,
               COUNT(*) AS disqualified_application
        FROM raw_events WHERE custom_track = 'unqualified'
        GROUP BY job_id, DATE(ts), HOUR(ts), publisher_id, campaign_id, group_id
    """)
    
    # Full Join các nhánh
    join_keys = ['job_id', 'dates', 'hours', 'publisher_id', 'campaign_id', 'group_id']
    aggregated_df = clicks_df \
        .join(conversions_df, on=join_keys, how='full') \
        .join(qualified_df, on=join_keys, how='full') \
        .join(unqualified_df, on=join_keys, how='full')
        
    # 3. Stream-to-Static Join với MySQL Job Metadata đã được cache & reload định kỳ
    job_meta = get_job_metadata(batch_spark)
    
    final_output = aggregated_df.join(broadcast(job_meta), 'job_id', 'left') \
        .drop(job_meta.group_id) \
        .drop(job_meta.campaign_id)
        
    # Chuẩn hóa cột
    final_output = final_output.select(
        'job_id', 'dates', 'hours', 'publisher_id', 'company_id', 'campaign_id', 'group_id',
        col('disqualified_application').cast('int'),
        col('qualified_application').cast('int'),
        col('conversion').cast('int'),
        col('clicks').cast('int'),
        'bid_set', 'spend_hour'
    ).na.fill({
        'disqualified_application': 0, 'qualified_application': 0, 
        'conversion': 0, 'clicks': 0, 'bid_set': 0.0, 'spend_hour': 0.0
    })
    
    # Thêm cột bổ trợ
    final_output = final_output.withColumn('sources', lit('Kafka-Streaming'))
    final_output = final_output.withColumn('updated_at', current_timestamp())
    logging.info(f"Đã hoàn thành Aggregation và Join Metadata. (Thời gian: {time.time() - t_start:.3f}s)")
    
    # 4. Ghi đè/Nạp vào MySQL events table
    t_start = time.time()
    def save_to_mysql():
        final_output.coalesce(2).write.format("jdbc") \
            .option("driver", MYSQL_DRIVER) \
            .option("url", MYSQL_URL) \
            .option("dbtable", MYSQL_TARGET_TABLE) \
            .mode("append") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .option("batchsize", "5000") \
            .option("isolationLevel", "NONE") \
            .save()
    execute_with_retry(save_to_mysql, target_name="MySQL", max_retries=3, initial_delay=2)

    logging.info(f"Đã nạp số liệu tổng hợp thời gian thực thành công vào MySQL. (Thời gian: {time.time() - t_start:.3f}s)")
    
    logging.info(f"=== Kết thúc Micro-batch {batch_id} - Tổng thời gian xử lý: {time.time() - start_time:.3f}s ===")

# Kết nối luồng stream đọc từ Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Phân giải giá trị Kafka (value) từ JSON sang Struct DataFrame
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), event_schema).alias("data")) \
    .select("data.*") \
    .filter(col("job_id").isNotNull())

# Khởi chạy luồng ghi Structured Streaming với trigger mỗi 10 giây để tối ưu tài nguyên và giảm tải database
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") \
    .start()

logging.info(">>> PySpark Structured Streaming đang hoạt động và lắng nghe Kafka...")
query.awaitTermination()