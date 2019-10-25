import json
import threading

from confluent_kafka import Consumer
from settings import *
from SparkFunctions import SparkFunctions


class TweetsConsumer:
    NUMBER_OF_THREADS = 2

    def __init__(self):
        self.topic_name = 'tweets'
        self.group_name = 'tweet_consumers'
        self.broker = 'localhost:9092'
        self.spark_obj = SparkFunctions()

    def create_kafka_consumer(self):
        try:
            print("Creating Consumer....")
            c = Consumer({
                'bootstrap.servers': self.broker,
                'group.id': self.group_name,
                # 'auto.offset.reset': 'earliest'
            })
            print("Consumer created successfully!")
            return c
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(ex)

    def subscribe_to_topic(self):
        try:
            self.consumer.subscribe([self.topic_name])
        except Exception as e:
            print("Unable to Subscribe to defined topic", e)

    def create_workers(self):
        thread_names = list()
        for _ in range(self.NUMBER_OF_THREADS):
            t = threading.Thread(target=self.run)
            thread_names.append(t)
            t.run()
        for t in thread_names:
            t.join()

    def run(self):
        self.consumer = self.create_kafka_consumer()
        self.subscribe_to_topic()
        print("Starting reading messages.....")
        print_filters = False
        while True:
            msg = self.consumer.poll(0.1)
            if msg is None:
                if print_filters:
                    for date in self.spark_obj.tweets_by_date:
                        print(f"{date}: {self.spark_obj.tweets_by_date[date]}")
                print_filters = False
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print_filters = True
            self.spark_obj.filter_by_date(tweets=json.loads(msg.value()))
            # print('Received message: {}'.format(msg.value()))


if __name__ == "__main__":
    TweetsConsumer().create_workers()
