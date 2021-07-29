from pyspark.sql import SparkSession

# create schema to file
spark = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()
df = spark.read.json("test_folder/discussions_0000001.json")
json_schema = df.schema.json()
f = open("extracted_schema", "w")
f.write(json_schema)
f.close()
print("Completed create_schema")



