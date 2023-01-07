from kafka import KafkaConsumer
import json


KAFKA_SERVER = 'localhost:29092'
TOPIC_ID = 'my_super_topic'
USER = "Ivan"


consumer = KafkaConsumer(TOPIC_ID, bootstrap_servers=KAFKA_SERVER)


print("Bootsraped", KAFKA_SERVER)


while True:
    for message in consumer:
        print("Incoming message")
        incoming_message = json.loads(message.value.decode())
        print(incoming_message['user_name'])
