from kafka import KafkaProducer
import time
import json
from datetime import datetime
import os


USER = os.getenv('USER')
TIMESTAMP = datetime.now()


KAFKA_SERVER = 'localhost:29092'
TOPIC_ID = 'my_super_topic'
TOPIC_TWO = 'my_favorit_topic'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


print('Started', KAFKA_SERVER)


for i in range(10):
    body = {
        "message_num": i,
        "timesatmp": str(TIMESTAMP),
        "item_num": i,
        "user_name": USER
    }
    producer.send(TOPIC_ID, json.dumps(body).encode("utf-8"))
    print("Message sent:", i, TOPIC_ID)
    time.sleep(1)
    producer.send(TOPIC_TWO, json.dumps(body).encode("utf-8"))
    print("Message sent:", i, TOPIC_TWO)
    time.sleep(1)
