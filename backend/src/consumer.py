from json import loads
from kafka import KafkaConsumer


class Consumer:
    def __init__(self, topic, group, KAFKA_BROKER_URI):
        self.topic = topic
        self.group = group
        
        decoder_FUNC = lambda x: loads(x.decode('utf-8'))


        print(f"consuming from URL: {KAFKA_BROKER_URI}")
        self._consumer = KafkaConsumer(self.topic, # topic name
                                        auto_offset_reset='earliest', # start reading at latest committed offset on break down/turn off
                                        enable_auto_commit=True, # commit offset every interval
                                        group_id=self.group, # consumer group id
                                        consumer_timeout_ms=1000,
                                        bootstrap_servers=KAFKA_BROKER_URI,
                                        value_deserializer=decoder_FUNC) # decode the data
        self._consumer.subscribe(topic)
        self.message_stack = [] # in case data is not found on first call

    def read_messages(self):
        messages = list(self._consumer)
        if len(messages) > 0:
            self.message_stack = [m.value for m in messages]
            # print("example message: ", self.message_stack[0])

            # logger.info(Fore.GREEN+f"consumed {len(self.message_stack)} messages"+Style.RESET_ALL)
            print(f"consumed {len(self.message_stack)} messages")
        else:
            print("no messages")
            

    def close(self):
        self._consumer.close()   