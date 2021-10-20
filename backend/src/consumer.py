from json import loads
from kafka import KafkaConsumer


class Consumer:
    def __init__(self, topic, group, KAFKA_BROKER_URI):
        self.topic = topic
        self.group = group
        self.KAFKA_BROKER_URI = KAFKA_BROKER_URI
        
        self.decoder_FUNC = lambda x: loads(x.decode('utf-8'))


        self.message_stack = [] # in case data is not found on first call
        self.connect()

    def connect(self):
        self._consumer = KafkaConsumer(self.topic, # topic name
                                        auto_offset_reset='earliest', # start reading at latest committed offset on break down/turn off
                                        enable_auto_commit=True, # commit offset every interval
                                        
                                        # group_id=self.group, # consumer group id
                                        consumer_timeout_ms=1000,
                                        bootstrap_servers=self.KAFKA_BROKER_URI,
                                        value_deserializer=self.decoder_FUNC) # decode the data
        
        self._consumer.subscribe(self.topic)

    def read_messages(self):
        messages = list(self._consumer)
        if len(messages) > 0:
            self.message_stack = [m.value for m in messages]
            print(f"consumed {len(self.message_stack)} messages")
        # else the message stack remains as it did to avoid no data being shown
        else:
            print("no messages")

    def close(self):
        self._consumer.close()   