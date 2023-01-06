from kafka import BrokerConnection
import time

broker = BrokerConnection('localhost', '9092')


while True:
    broker.connect()
