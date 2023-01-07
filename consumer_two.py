from kafka import KafkaConsumer
import json
import os


USER = os.getenv('USER')


KAFKA_SERVER = 'localhost:29092'


TOPIC_TWO = 'Second_Topic'


consumer = KafkaConsumer(TOPIC_TWO, bootstrap_servers=KAFKA_SERVER)


print("Bootsraped", KAFKA_SERVER)

# Ждет сообщения из топика - 2
while True:
    for message in consumer:
        print("Incoming message")
        incoming_message = json.loads(message.value.decode())
        print("Consumer_two:", incoming_message['user_name'],
              incoming_message.keys(), incoming_message.values())
