from kafka import KafkaConsumer

bootstrap_server = ['127.0.0.1:9092']

topicName = 'raw_data'

consumer = KafkaConsumer(topicName,
                         bootstrap_servers=bootstrap_server,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

for msg in consumer:
    print(str(msg.topic) + 'P' + str(msg.partition) + 'OFF' + str(msg.offset), msg.value)
    #print(type(msg.value))
