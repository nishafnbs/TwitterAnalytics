from pyspark import SparkContext, SparkConf
from settings import *


class SparkFunctions:
    def __init__(self):
        self.tweets_by_date = dict()
        self.sc = self.initialize_spark()

    @staticmethod
    def get_spark_conf():
        return SparkConf().setAppName("Tweets").setMaster("local[*]")

    def initialize_spark(self):
        return SparkContext(conf=self.get_spark_conf())

    def filter_by_date(self, tweets):
        data = self.sc.parallelize(tweets)
        data = data.map(lambda word: (word['publish_date'].split()[0].strip(), 1))
        tweets_by_date = data.reduceByKey(lambda x, y: x + y)
        tweets_by_date = tweets_by_date.sortBy(lambda d: int(d[1]), ascending=False)
        for tweet_date, count in tweets_by_date.collect():
            if tweet_date in self.tweets_by_date:
                self.tweets_by_date[tweet_date] += count
            else:
                self.tweets_by_date[tweet_date] += count
            # if 'publish_date' in tweet:
        #     publish_date = tweet['publish_date'].split()[0].strip()
        #     if publish_date in self.tweets_by_date:
        #         self.tweets_by_date[publish_date] += 1
        #     else:
        #         self.tweets_by_date[publish_date] = 1



