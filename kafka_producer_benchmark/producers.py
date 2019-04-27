from confluent_kafka import Producer

from .settings import KAFKA_CONFIG
from .timeit import timeit


class BaseProducer():
    def produce(self, topic:str, value:str, key:str):
        return 0

    def done(self):
        return 0


class VolitileProducer(BaseProducer):
    ''' Producer gets created and discarded after sending a single message. '''

    @timeit
    def produce(self, topic:str, value:str, key:str):
        producer = Producer(KAFKA_CONFIG)
        producer.produce(topic, value, key)
        producer.flush()


class PersistentProducer(BaseProducer):
    ''' Persistent producer, flush occurs after each produce call. '''
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)

    @timeit
    def produce(self, topic:str, value:str, key:str):
        self.producer.produce(topic, value, key)
        self.producer.flush()


class PersistentAsyncProducer(BaseProducer):
    ''' Persistent producer, which flushes once per session. '''
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)

    @timeit
    def produce(self, topic:str, value:str, key:str):
        self.producer.produce(topic, value, key)

    @timeit
    def done(self):
        self.producer.flush()
