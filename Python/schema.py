from pyspark.sql.types import *

rawDataSchema = StructType([
    StructField("master_uuid", LongType()),
    StructField("author", StringType()),
    StructField("url", StringType()),
    StructField("ord_in_thread", IntegerType()),
    StructField("title", StringType()),
    StructField("locations", StringType()),
    StructField("entities", StringType()),
    StructField("locations", StringType()),
    StructField("organizations", StringType()),
    StructField("highlightText", StringType()),
    StructField("language", StringType()),
    StructField("persons", StringType()),
    StructField("text", StringType()),
    StructField("external_links", StringType()),
    StructField("published", TimestampType()),
    StructField("crawled", TimestampType()),
    StructField("highlightTitle", StringType())
])

threadDataSchema = StructType([
    StructField("master_uuid", LongType()),
    StructField("site_full", StringType()),
    StructField("main_image", StringType()),
    StructField("site_section", StringType()),
    StructField("section_title", StringType()),
    StructField("url", StringType()),
    StructField("country", StringType()),
    StructField("title", StringType()),
    StructField("performance_score", FloatType()),
    StructField("site", StringType()),
    StructField("participants_count", IntegerType()),
    StructField("title_full", StringType()),
    StructField("spam_score", FloatType()),
    StructField("site_type", StringType()),
    StructField("published", TimestampType()),
    StructField("replies_count", IntegerType()),
    StructField("uuid", LongType())
])

socialDataSchema = StructType([
    StructField("master_uuid", LongType()),
    StructField("gplus_shares", LongType()),
    StructField("pinterest_shares", LongType()),
    StructField("vk_shares", LongType()),
    StructField("linkedin_shares", LongType()),
    StructField("facebook_likes", LongType()),
    StructField("facebook_shares", LongType()),
    StructField("facebook_comments", LongType()),
    StructField("stumbleupon_shares", LongType()),
])
