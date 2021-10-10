
# # standard
# import logging
# # from tqdm import tqdm
# from time import sleep
# from typing import Iterable
# from json import dumps, loads

# # data engineering
# from pymongo import MongoClient
# from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient # avoid script name error with import
# from kafka.admin import NewTopic

# # polish
# from colorama import Fore, Style

# module
from caller import Caller


import os
from time import sleep

from apscheduler.schedulers.blocking import BlockingScheduler

# from pymongo import MongoClient

from kafka.admin.new_topic import NewTopic

from producer import Producer



global dev
dev = 0

# def instantiate_topic(topic_name, group_name):
    # checking if this topic needs to be instantiated
    # _ = Consumer(topic_name, group_name, KAFKA_BROKER_URI)
    # if topic_name not in _.topics():
    #     # logging.info(Fore.GREEN+"instantiating topic"+Style.RESET_ALL)
    #     print("instantiating topic")
       
    #     topic = NewTopic(name=topic_name, num_partitions=1)
        
    #     admin = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
    #     admin.create_topic(topic)
    # else:
    #     logger.info(Fore.GREEN+"topic exists, not instantiating"+Style.RESET_ALL)

# def backup_to_db(client):
    # database persistance
    # print("MONGO", flush=True)
    # client = MongoClient("mongo:27017")
    # try:
    #     client.admin.command('ismaster')
    #     print("SERVER SUCCESS")
    # except:
    #     print("ERRRRR: Server not available")

def run_minute_update(spider, topic, KAFKA_BROKER_URI):
    """
    call for data and publish to the kafka producer
    """
    data = spider.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
    producer_ = Producer(topic, KAFKA_BROKER_URI)
    

    # global dev
    # dev +=1 
    producer_._publish(data) #{"counter":dev}
    
    # producer_.producer_.flush()
    # producer_.producer_.close()


if __name__ == '__main__':
    topic = "quake"
    KAFKA_BROKER_URI = os.environ.get("KAFKA_BROKER_URI") 
    print(f"using broker URI: {KAFKA_BROKER_URI}")

    

    # DEV 
    


    # schedule continual updates (streams online but only pulling every minute when USGS updates data)
    # spider = Caller()
    # schedule = BlockingScheduler()
    # ADD: change back to 60
    # schedule.add_job(run_minute_update, "interval", args=[spider, topic, KAFKA_BROKER_URI], seconds=10, max_instances=2)
    # schedule.start()