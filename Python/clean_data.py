from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import new_schema

db_target_properties = {"user": "root", "password": "jordan", "driver": 'com.mysql.cj.jdbc.Driver'}


def consume():
    print("begin consume")
    bootstrap_server = "127.0.0.1:9092"
    topicName = "raw_data"

    # create spark session
    spark = SparkSession.builder.appName("SparkKafkaConsumer_clean") \
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
    # clean_data.printSchema()

    # load data into db
    # loading clean_data into table clean_data
    def foreach_batch_function_clean(df, epoch_id):
        print("Begin write to DB")
        df.write.jdbc(url='jdbc:mysql://localhost:3306/capstone_project',
                      table="clean_data", properties=db_target_properties, mode="overwrite")
        print("Complete write to DB")
        pass
    clean_load = clean_data.writeStream.outputMode("append").foreachBatch(foreach_batch_function_clean).start()
    # debug : testing print to console what was selected
    # stream_test = transformed \
    #     .writeStream \
    #     .outputMode("update") \
    #     .option("truncate", "false") \
    #     .format("console") \
    #     .start()
    # stream_test.awaitTermination()

    print("end consume")
    return clean_load
