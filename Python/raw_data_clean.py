from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import new_schema
from credentials import user, password

db_target_properties = {"user": user, "password": password, "driver": 'com.mysql.cj.jdbc.Driver'}


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

    thread_data = data.selectExpr("raw_data.uuid as master_uuid",
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

    clean_data = data.selectExpr("raw_data.uuid as master_uuid",
                                 "raw_data.author",
                                 "raw_data.crawled",
                                 "raw_data.entities.locations[0] as entities_locations",
                                 "raw_data.entities.organizations[0] as entities_organizations",
                                 "raw_data.entities.persons[0] as entities_persons",
                                 "raw_data.external_links[0] as external_links",
                                 "raw_data.highlightText",
                                 "raw_data.highlightTitle",
                                 "raw_data.language",
                                 "raw_data.locations[0] as locations",
                                 "raw_data.ord_in_thread",
                                 "raw_data.organizations[0] as organizations",
                                 "raw_data.persons[0] as persons",
                                 "raw_data.published",
                                 "raw_data.text",
                                 "raw_data.title",
                                 "raw_data.url"
                                 )
    # social_data.printSchema()
    # thread_data.printSchema()
    # clean_data.printSchema()

    # load data into db
    # loading social_data into table social_data
    def foreach_batch_function_social(df, epoch_id):
        print("Begin social_data write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="social_data", properties=db_target_properties, mode="overwrite")
        print("Complete social_data write to DB")
        pass

    # loading thread_data into table thread_data
    def foreach_batch_function_thread(df, epoch_id):
        print("Begin thread_data write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="thread_data", properties=db_target_properties, mode="overwrite")
        print("Complete thread_data write to DB")
        pass

    # loading clean_data into table clean_data
    def foreach_batch_function_clean(df, epoch_id):
        print("Begin clean_data write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="clean_data", properties=db_target_properties, mode="overwrite")
        print("Complete clean_data write to DB")
        pass

    social_load = social_data.writeStream.outputMode("update").foreachBatch(foreach_batch_function_social).start()
    thread_load = thread_data.writeStream.outputMode("update").foreachBatch(foreach_batch_function_thread).start()
    clean_load = clean_data.writeStream.outputMode("update").foreachBatch(foreach_batch_function_clean).start()
    spark.streams.awaitAnyTermination()

    # debug : testing print to console what was selected
    # stream_test = transformed \
    #     .writeStream \
    #     .outputMode("update") \
    #     .option("truncate", "false") \
    #     .format("console") \
    #     .start()
    # stream_test.awaitTermination()
    return "Consume Complete"
