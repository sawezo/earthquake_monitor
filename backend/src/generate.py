# import random
# import time, calendar
# from random import randint
# from kafka import KafkaProducer
# from kafka import errors 
# from json import dumps
import os


import sys
sys.path.append("/src/")
from producer import Producer
import json
from time import sleep


KAFKA_BROKER_URI = os.environ.get("KAFKA_BROKER_URI")


if __name__ == '__main__':
    producer = Producer(topic="quake", KAFKA_BROKER_URI=KAFKA_BROKER_URI)
    

    with open("/src/dev/test.json", "r") as infile:
        test_data = json.load(infile)
    while True:
        producer.publish(test_data)
        print("message sent")
        sleep(3)