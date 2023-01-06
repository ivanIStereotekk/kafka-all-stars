from kafka import KafkaConsumer


KAFKA_SERVER = 'localhost:9092'
consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER)


message = "message which i sent"
topic = 'my_super_topic'

for msg in consumer:
    print(msg)
