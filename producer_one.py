from kafka import KafkaProducer
import time
import json
from datetime import datetime
import os


USER = os.getenv('USER')
TIMESTAMP = datetime.now()


KAFKA_SERVER = 'localhost:29092'


producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
TOPIC_ONE = 'Topic_One'
print('Bootstraped server:', KAFKA_SERVER)

# продюсер 1 - отправляет сообщения в топик 1


for i in range(10):
    message_body = {
        "message_num": i,
        "timesatmp": str(TIMESTAMP),
        "item_num": i,
        "user_name": USER
    }
    producer.send(TOPIC_ONE, json.dumps(message_body).encode("utf-8"))
    print(f"Message sent:{message_body}", i, TOPIC_ONE)
    time.sleep(1)
