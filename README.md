# springboard_capstone_0
springboard capstone project commands
1. start zookeeper instance
zookeeper-server-start.bat config\zookeeper.properties
2. start kafka instance
kafka-server-start.bat config\server.properties
3. create topic raw_data
kafka-topics --zookeeper 127.0.0.1:2181 --topic raw_data --create --partitions 3 --replication-factor 1
4. spark submit
spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,mysql:mysql-connector-java:8.0.26 main.py
5. start Metabase instance, in the directory of the metabase jar file	
java -jar metabase.jar
