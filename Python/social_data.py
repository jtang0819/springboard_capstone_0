from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import new_schema

db_target_properties = {"user": "root", "password": "jordan", "driver": 'com.mysql.cj.jdbc.Driver'}


def consume():
    print("begin consume")
    bootstrap_server = "127.0.0.1:9092"
    topicName = "raw_data"

    # create spark session
    spark = SparkSession.builder.appName("SparkKafkaConsumer_social") \
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

    # select data for sql db
    social_data = data.selectExpr("raw_data.uuid as master_uuid",
                                  "raw_data.thread.social.facebook.comments as facebook_comments",
                                  "raw_data.thread.social.facebook.likes as facebook_likes",
                                  "raw_data.thread.social.facebook.shares as facebook_shares",
                                  "raw_data.thread.social.gplus.shares as gplus_shares",
                                  "raw_data.thread.social.linkedin.shares as linkedin_shares",
                                  "raw_data.thread.social.pinterest.shares as pinterest_shares",
                                  "raw_data.thread.social.stumbledupon.shares as stumbledupon_shares",
                                  "raw_data.thread.social.vk.shares as vk_shares"
                                  )

    # load data into db
    # loading social_data into table social_data
    def foreach_batch_function_social(df, epoch_id):
        print("Begin write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="social_data", properties=db_target_properties, mode="overwrite")
        print("Complete write to DB")
        pass
    social_load = social_data.writeStream.outputMode("append").foreachBatch(foreach_batch_function_social).start()

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
