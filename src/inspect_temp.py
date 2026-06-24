import mysql.connector
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Config
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "123"
MYSQL_DATABASE = "etl_database"

CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042
CASSANDRA_USER = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "recruitment"

def inspect_mysql():
    print("--- MYSQL INSPECTION ---")
    cnx = mysql.connector.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE
    )
    
    # Check master_publisher
    pub_df = pd.read_sql("SELECT * FROM master_publisher", cnx)
    print("Publishers in MySQL:")
    print(pub_df)
    
    # Check jobs
    jobs_df = pd.read_sql("SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT campaign_id) FROM job", cnx)
    print("\nJobs summary in MySQL:")
    print(jobs_df)
    
    # Check events distribution
    events_df = pd.read_sql("SELECT publisher_id, SUM(clicks) as total_clicks, COUNT(*) as record_count FROM events GROUP BY publisher_id", cnx)
    print("\nEvents distribution by publisher in MySQL:")
    print(events_df)
    
    cnx.close()

def inspect_cassandra():
    print("\n--- CASSANDRA INSPECTION ---")
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    
    rows = session.execute("SELECT publisher_id, custom_track FROM tracking LIMIT 10000")
    df = pd.DataFrame(list(rows))
    if not df.empty:
        print("Cassandra tracking distribution by publisher:")
        print(df.groupby('publisher_id').size())
        print("\nCassandra tracking distribution by custom_track:")
        print(df.groupby('custom_track').size())
    else:
        print("Cassandra tracking table is empty.")
    cluster.shutdown()

if __name__ == "__main__":
    inspect_mysql()
    inspect_cassandra()
