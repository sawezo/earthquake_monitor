import os
import sys
import json
from time import sleep

sys.path.append("/src/")
from pull import Puller
from utils import frame_data
from producer import Producer
from consumer import Consumer
from mongo import MongoDatabase


KAFKA_BROKER_URI = os.environ.get("KAFKA_BROKER_URI")
TOPIC = os.environ.get("TOPIC")
# GROUP = os.environ.get("GROUP")

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

keep2rename = {'properties>mag':'magnitude',
               'properties>sig':'significance',
               'geometry>coordinates>0':'longitude',
               'geometry>coordinates>1':'latitude',
               'geometry>coordinates>2':'depth'}


# from apscheduler.schedulers.blocking import BlockingScheduler
# schedule continual updates (streams online but only pulling every minute when USGS updates data)
# spider = Caller()
# schedule = BlockingScheduler()
# ADD: change back to 60
# schedule.add_job(run_minute_update, "interval", args=[spider, topic, KAFKA_BROKER_URI], seconds=10, max_instances=2)
# schedule.start()

if __name__=="__main__":    
    sleep(30)

    # usgs
    spider = Puller()


    # kafka
    producer = Producer(topic=TOPIC, KAFKA_BROKER_URI=KAFKA_BROKER_URI)
    consumer = Consumer(topic=TOPIC, group="quake", KAFKA_BROKER_URI=KAFKA_BROKER_URI)
    

    # mongo
    mongo = MongoDatabase(db_name=TOPIC)
    mongo.select_collection(collection_name="geo_backup")
    mongo.geo_index()


    # read test data (avoid api in testing)
    # with open("/src/dev/test.json", "r") as infile:
    #     data = json.load(infile)


    # logic loop
    while True:
        # update data pull
        data = spider.get(URL)
        
        # flattening document (easier here than in Flink SQL)
        # this may need to be brought into a kafka line ... at least the main producer <- just do that...  
        df = frame_data(data["features"], keep2rename)

        # send each earthquake to the producer
        for row in df.to_dict(orient="records"):
            producer.publish(row)

        # update the consumer message stack
        consumer.read_messages()

        quakes = consumer.message_stack
        mongo.collection.insert_many(quakes)    
        sleep(60)