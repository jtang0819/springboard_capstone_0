# import findspark
# #initialization
# findspark.init()
# import warnings
# warnings.filterwarnings('ignore')
# from pyspark.sql import SparkSession
# # Define the address of the database, as well as the table, login user and password
# url = "jdbc:mysql://localhost:3306/capstone_project"
# table="social_data"
# #Password account needs to be passed in as a dictionary
# properties ={"user":"root","password":"jordan"}
# spark = SparkSession.builder.appName('My first app').getOrCreate()
# df = spark.read.jdbc(url=url,table=table,properties=properties)
# df.show()