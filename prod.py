from kafka import KafkaProducer
import time
import json
from datetime import datetime
TIMESTAMP = datetime.now()
KAFKA_SERVER = 'localhost:29092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


print('Started', KAFKA_SERVER)

TOPIC_ID = 'my_super_topic'

for i in range(10):
    body = {
        "message_num": i,
        "timesatmp": TIMESTAMP,
        "item_num": i
    }
    producer.send(TOPIC_ID, json.dumps(body).encode("utf-8"))
    print("Message sent:", i)
    time.sleep(1)


# docker run -d --name kafka-container -e TZ=UTC -p 29092:29092 -e ZOOKEEPER_HOST=host.docker.internal ubuntu/kafka:3.1-22.04_beta
