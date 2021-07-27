from pyspark.sql import SparkSession
from schema import rawDataSchema, threadDataSchema, socialDataSchema


# db_target_properties = {"user":"xxxx", "password":"yyyyy"}


# def foreach_batch_function(df, epoch_id):
#     print(df.collect())
#     # df.write.jdbc(url='jdbc:mysql://172.16.23.27:30038/securedb',  table="sparkkafka",  properties=db_target_properties)
#     pass


def consume():
    bootstrap_server = "127.0.0.1:9092"
    topicName = "raw_data"

    # create spark session
    spark = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # create streaming dataframe
    data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", topicName) \
        .option("startingOffsets", "earliest") \
        .load()

    transformed = data.selectExpr("CAST(value AS STRING)")
    schema = transformed.printSchema()

    # select data for sql db
    # stream = transformed.writeStream \
    #     .format("csv") \
    #     .option("format", "append") \
    #     .option("path", "checkpointlocation") \
    #     .option("checkpointLocation", "checkpointlocation") \
    #     .queryName("test") \
    #     .outputMode("append") \
    #     .start() \
        #.awaitTermination()
    # .foreachBatch(foreach_batch_function) \

    # testing print to console what was selected
    stream_test = schema \
        .writeStream \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    stream_test.awaitTermination()
    return schema



# test
#         .writeStream \
#         .trigger(processingTime='1 seconds') \
#         .outputMode("update") \
#         .option("truncate", "false")\
#         .format("console") \
#         .start()
# consumer = KafkaConsumer(topicName,
#                          bootstrap_servers=bootstrap_server,
#                          auto_offset_reset='earliest',
#                          enable_auto_commit=True)
# for msg in consumer:
#     print(str(msg.topic) + 'P' + str(msg.partition) + 'OFF' + str(msg.offset), msg.value)
# print(type(msg.value))
