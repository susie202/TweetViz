import sys
import json
import os

from pyspark.sql import SparkSession

from pyspark.sql.functions import expr, monotonically_increasing_id, col, row_number, explode, from_json
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, IntegerType, StringType, MapType
import uuid
from pyspark.sql.functions import udf

from google.cloud import bigquery

credential_path = "/Users/seoyeong/Downloads/tweetdeck-320105-9820690ac16e.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


# ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar SparkStreaming.py localhost:9092 subscribe twitterdata
# --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
# --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar
# /usr/local/Cellar/apache-spark/3.1.2/libexec/jars/spark-bigquery-with-dependencies_2.11-0.21.1.jar 
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    client = bigquery.Client()
    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector. 
    bucket = "kafka-sparkstream-bigquery"

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaTweetData")\
        .config("spark.jars", "") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    

    # Create DataSet representing the stream of input lines from kafka
    # Kafka 클러스터 위치, 데이터 읽으려는 토픽 지정
    # spark.readStream () ; DataStreamReader 인터페이스 리턴해주면서 Streaming DataFrames 생성함
    df_json = spark\
        .read\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .option("startingOffsets", "earliest")\
        .option("endingOffsets", "latest")\
        .option("failOnDataLoss", "false")\
        .load()
    # filter out empty values
    df_json = df_json.withColumn("value", expr("string(value)"))\
        .filter(col("value").isNotNull())\
    # get latest version of each record
    df_json = df_json.select("key", expr("struct(offset, value) r"))\
        .groupBy("key").agg(expr("max(r) r"))\
        .select("r.value")
    
    # decode the json values
    df = spark.read.json(
        df_json.rdd.map(lambda x: x.value), multiLine=True)

    # drop corrupt records
    if "_corrupt_record" in df.columns:
        df = (df
                .filter(col("_corrupt_record").isNotNull())
                .drop("_corrupt_record"))

    json_schema = df.schema.json()
    print(json_schema)

    obj = json.loads(json_schema)
    topic_schema = StructType.fromJson(obj)
    print(topic_schema)
       
    parsed = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option(subscribeType, topics) \
    .load() \
    .select(from_json(col("value").cast("string"), topic_schema))

    parsed = parsed.select("from_json(CAST(value AS STRING)).*")
    
    print("result")

    
    table_id = "tweetdeck.tweetdeck_dataset.tweetdeck_table"
    # Update to your GCS bucket
    gcs_bucket = 'kafka-sparkstream-bigquery'

    # Update to your BigQuery dataset name you created
    bq_dataset = 'tweetdeck_dataset'
    # Enter BigQuery table name you want to create or overwite. 
    # If the table does not exist it will be created when you run the write function
    bq_table = 'tweetdeck_table'
    query = parsed\
        .writeStream\
        .format('bigquery')\
        .option("temporaryGcsBucket","kafka-sparkstream-bigquery") \
        .option("checkpointLocation", "gs://kafka-sparkstream-bigquery-checkpoint/checkpoint") \
        .option("table","tweetdeck_dataset.tweetdeck_table") \
        .mode("append") \
        .start()
    query.awaitTermination()
    
    '''
            .option("credentialsFile", "/Users/seoyeong/Downloads/tweetdeck-320105-9820690ac16e.json") \

    # BigQuery 클라이언트를 초기화하여 BigQuery API를 인증하고 연결
    # bigquery.Client 클래스를 인스턴스화하여 BigQuery 클라이언트를 만듭니다.
    client = bigquery.Client()
    # Todo(developer): Set table_id to the ID of table to append to.
    # table_id = "your-project.your_dataset.your_table"

    rows_to_insert = [
        {u"full_name": u"Phred Phlyntstone", u"age": 32},
        {u"full_name": u"Wylma Phlyntstone", u"age": 29},
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    '''

