from json import dumps
from kafka import KafkaProducer


class Producer:
    def __init__(self, topic, KAFKA_BROKER_URI):
        self.topic = topic

        serialize_FUNC = lambda x: dumps(x).encode('utf-8') # how to serialize data before sending to broker
        self.producer_ = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URI, 
                                       api_version=(0, 10), 
                                       value_serializer=serialize_FUNC)
    
    
    def report_success(self, record_metadata):
        # logger.info(Fore.GREEN+"message sent"+Style.RESET_ALL)
        pass
    def report_failure(self, excp):
        # logger.error('Error in data producer', exc_info=excp)
        print('Error in data producer', excp)

    def _publish(self, data):
        self.producer_.send(self.topic, value=data).add_errback(self.report_failure).add_callback(self.report_success)

    def close(self):
        self.producer_.close()