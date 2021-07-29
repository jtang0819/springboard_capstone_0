from pyspark.sql.types import *
import json

# create schema variable from extracted_schema
with open("extracted_schema") as j:
    new_schema = StructType.fromJson(json.load(j))
    j.close()
