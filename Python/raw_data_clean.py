from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import new_schema

db_target_properties = {"user": "root", "password": "jordan", "driver": 'com.mysql.cj.jdbc.Driver'}


def consume():
    print("begin consume")
    bootstrap_server = "127.0.0.1:9092"
    topicName = "raw_data"

    # create spark session
    spark = SparkSession.builder.appName("SparkKafkaConsumer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # create streaming dataframe
    data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", topicName) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), new_schema).alias("raw_data"))

    print("Print schema of data")
    # TODO rename uuid to master_uuid
    data.withColumnRenamed("raw_data.uuid", "master_uuid").printSchema()
    # data1.printSchema()
    # data.printSchema()
    # df = data.withColumnRenamed("uuid", "master_uuid")
    # df.printSchema()

    # select data for sql db
    social_data = data.select("raw_data.uuid", "raw_data.thread.social.*")
    social_data.printSchema()
    thread_data = data.select("raw_data.uuid",
                              "raw_data.thread.site_full",
                              "raw_data.thread.main_image",
                              "raw_data.thread.site_section",
                              "raw_data.thread.section_title",
                              "raw_data.thread.url",
                              "raw_data.thread.country",
                              "raw_data.thread.title",
                              "raw_data.thread.performance_score",
                              "raw_data.thread.site",
                              "raw_data.thread.participants_count",
                              "raw_data.thread.title_full",
                              "raw_data.thread.spam_score",
                              "raw_data.thread.site_type",
                              "raw_data.thread.published",
                              "raw_data.thread.replies_count",
                              "raw_data.thread.uuid")
    thread_data.printSchema()

    # social_data.printSchema()
    # thread_data.printSchema()
    # load data into db
    # loading social_data into table social_data

    def foreach_batch_function(df, epoch_id):
        print("Begin write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="social_data", properties=db_target_properties, mode="append")
        print("Complete write to DB")
        pass
    social_load = social_data.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
    social_load.awaitTermination()


    # debug : testing print to console what was selected
    # stream_test = transformed \
    #     .writeStream \
    #     .outputMode("update") \
    #     .option("truncate", "false") \
    #     .format("console") \
    #     .start()
    # stream_test.awaitTermination()

    print("end consume")
    return social_load
