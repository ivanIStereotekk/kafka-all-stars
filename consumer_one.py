from kafka import KafkaConsumer
import json
import os


USER = os.getenv('USER')


KAFKA_SERVER = 'localhost:29092'
TOPIC_ONE = 'Topic_One'


consumer = KafkaConsumer(TOPIC_ONE, bootstrap_servers=KAFKA_SERVER)


print("Bootsraped", KAFKA_SERVER)


while True:
    for message in consumer:
        print("Incoming message")
        incoming_message = json.loads(message.value.decode())
        print("Consumer_one --",
              incoming_message['user_name'], incoming_message.keys())
