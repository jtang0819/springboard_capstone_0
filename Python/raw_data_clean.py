from pyspark.sql import SparkSession
from schema import rawDataSchema, threadDataSchema, socialDataSchema

def consume():
    bootstrap_server = "127.0.0.1:9092"
    topicName = "raw_data"

    # create spark session
    spark = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()
    # create streaming dataframe
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", topicName) \
        .load() \
        .selectExpr("CAST(value AS STRING)")
    # .option("startingOffsets", "raw_data") \

    # select data for sql db

    query = df.writeStream \
        .format("console") \
        .queryName("test") \
        .outputMode("update") \
        .start() \
        .awaitTermination()

    return query

# consumer = KafkaConsumer(topicName,
#                          bootstrap_servers=bootstrap_server,
#                          auto_offset_reset='earliest',
#                          enable_auto_commit=True)
# for msg in consumer:
#     print(str(msg.topic) + 'P' + str(msg.partition) + 'OFF' + str(msg.offset), msg.value)
# print(type(msg.value))
