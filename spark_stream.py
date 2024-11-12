import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, StructType




def create_keyspace(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS spark_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    print('Keyspace created successfully')

def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.created_articles(
        id UUID PRIMARY KEY,
        source TEXT,
        author TEXT,
        title TEXT,
        publish_date DATE
        )
        """
    )

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data")
    id = kwargs.get('id')
    source = kwargs.get('source')
    author = kwargs.get('author')
    title = kwargs.get('title')
    publish_date = kwargs.get('publish_date')

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_articles(id,source,author,title,publish_date)
                VALUES (%s,%s,%s,%s,%s)
            """, (id,source,author,title,publish_date))
        logging.info(f"Data inserted for {source} {author}")
    except Exception as e:
        logging.error(f"Couldnt insert data for {source} {author} due to exception: {e}")

    print('Data inserted successfully')
    return

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Connection created successfully!")


    except Exception as e:
        logging.error("Couldn't create the spark session due to exception: {e} ")
        raise
    if s_conn is None:
        raise Exception("Failed to create spark connection")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers','broker:29092') \
            .option('subscribe','users_created') \
            .option('startingOffsets','earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    # connecting to cassandra cluster
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error("Could not create cassandra connection due to {e}")

    return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id",StringType(), False),
        StructField("source", StringType(), False),
        StructField('author',StringType(), False),
        StructField('title',StringType(), False),
        StructField('publish_date', StringType(), False)
    ])
    df = spark_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select(
        "data.*")
    print(df)

    return df


if __name__ == "__main__":
    #create spark connection
    spark_conn = create_spark_connection()
    authentication = PlainTextAuthProvider(username='admin', password='admin')

    if spark_conn is not None:
        # connect ot kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format('org.apache.spark.sql.cassandra')
                             .option("checkpointLocation", '/tmp/checkpoint')
                             .option('keyspace', 'spark_streams')
                             .option('table', 'created_articles')
                             .start())

            streaming_query.awaitTermination()
