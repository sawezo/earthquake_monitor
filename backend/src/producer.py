from json import dumps
from kafka import KafkaProducer
from time import sleep


class Producer:
    def __init__(self, topic, KAFKA_BROKER_URI, wait_time=10):
        self.topic = topic
        self.KAFKA_BROKER_URI = KAFKA_BROKER_URI
        self.serialize_FUNC = lambda x: dumps(x).encode('utf-8') # how to serialize data before sending to broker
        
        self.connect(wait_time)
        
    def connect(self, wait_time):
        while True:
            try:
                self.producer_ = KafkaProducer(bootstrap_servers=self.KAFKA_BROKER_URI, 
                                               value_serializer=self.serialize_FUNC)
                print("Connected to Kafka", flush=True)
                break
            
            except:
                print("waiting for broker to come online")
                sleep(wait_time)
                pass

    def report_success(self, record_metadata):
        print("message produced")

    def report_failure(self, exp):
        print('message failed: ', exp)

    def publish(self, data):
        self.producer_.send(self.topic, value=data).add_errback(self.report_failure).add_callback(self.report_success)

    def close(self):
        self.producer_.close()