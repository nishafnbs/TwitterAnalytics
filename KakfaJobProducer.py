import csv
import json
import os
from itertools import islice
from settings import *
import numpy as np

from confluent_kafka import Producer, admin

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


class CreateTweetJobs:
    def __init__(self):
        self.topic_name = 'tweets'
        self.filename = "IRAhandle_tweets_1.csv"
        self.HEADERS = list()
        self.broker = 'localhost:9092'
        self.admin_client = self.create_kafka_admin()
        self.producer = self.create_kafka_producer()

    def create_topics(self, topics, number_of_partitions=3, replication_factor=1):
        new_topics = [
            admin.NewTopic(topic, num_partitions=number_of_partitions, replication_factor=replication_factor)
            for topic in topics
        ]
        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = self.admin_client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def create_kafka_producer(self):
        try:
            print("Connecting to Kafka Server....")
            producer = Producer({'bootstrap.servers': self.broker, 'enable.idempotence': 'true'})
            print("Successfully Connected!")
            return producer
        except Exception as ex:
            print('Exception while connecting Kafka')
            raise

    def create_kafka_admin(self):
        try:
            print("Connecting to Kafka Server....")
            admin_client = admin.AdminClient({'bootstrap.servers': self.broker})
            print("Successfully Connected!")
            return admin_client
        except Exception as ex:
            print('Exception while connecting Kafka')
            raise
        # Create Admin client

    def read_tweets(self):
        with open(f'{os.getcwd()}/dataset/{self.filename}', 'r') as f:
            csv_reader = csv.reader(f)
            self.HEADERS = next(csv_reader)
            # d = [row for row in csv_reader]
            # print(len(d))
            for row in csv_reader:
                tweet_dict = self.get_tweet_dict(row)
                yield tweet_dict

    def get_tweet_dict(self, tweet):
        tweet_dict = dict()
        for index, header in enumerate(self.HEADERS):
            tweet_dict[header] = tweet[index]

        return tweet_dict

    def divide_in_chunks(self, it, size):
        it = iter(it)
        return iter(lambda: tuple(islice(it, size)), ())

    def run(self):
        # self.create_topics(self.topic_name)
        count = 0
        total_tweets = [tweet for tweet in self.read_tweets()]
        splitted_tweets = self.divide_in_chunks(total_tweets, 50)
        for tweets in splitted_tweets:
            self.producer.poll(0.1)
            self.producer.produce(self.topic_name, json.dumps(tweets))
        # for index in range(50):
        #     self.producer.poll(0.1)
        #     self.producer.produce(self.topic_name, str(index).encode('utf-8'))


        print(count)


if __name__ == "__main__":
    CreateTweetJobs().run()
