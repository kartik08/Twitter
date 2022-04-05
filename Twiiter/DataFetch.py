import tweepy
import os
import json
from Kakfa.kafkaProducer import producer
from Mongo import  mongo
apiKey = os.environ.get("APIKEY")
apiSecretKey = os.environ.get("APISECRETKEY")
accessToken = os.environ.get("ACCESSTOKEN")
accessTokenSecret = os.environ.get("ACCESSTOKENSECRET")
TWEET_KEYWORDS = ["web3"]
TOPIC = "Twitter"
class  MyStreamListener(tweepy.Stream):
    def on_data(self, raw_data):
        try:
            tweet_data = json.loads(raw_data)

            if "extended_tweet" in tweet_data:
                msg = tweet_data["extended_tweet"]["full_text"] + " t_end"
                # convert data to bytes and load into kafka producer
                collection.insert_one({"Text":msg})
                tweet = bytearray(str(msg).encode("utf-8"))
                producer.send(TOPIC, tweet)

            else:
                msg = tweet_data["text"] + " t_end"
                collection.insert_one({"Text": msg})
                # convert data to bytes and load into kafka producer
                tweet = bytearray(str(msg).encode("utf-8"))
                producer.send(TOPIC, tweet)

        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_request_error(self, status_code):
        print(status_code)
        return False


def stream_tweets(word):
    stream_listener = MyStreamListener(access_token=accessToken, access_token_secret=accessTokenSecret,
                                       consumer_key=apiKey, consumer_secret=apiSecretKey)
    stream_listener.filter(track=word, languages=["en"])

client = mongo.mongoConnection()
db = mongo.createDB("Twitter",client)
collection = mongo.createCollection("Raw Data",db)
stream_tweets(TWEET_KEYWORDS)
