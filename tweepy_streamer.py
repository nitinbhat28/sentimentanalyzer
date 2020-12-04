# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 10:30:11 2020

@author: MY PC
"""
from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from textblob import TextBlob

import twitter_credentials
import numpy as np
import pandas as pd
import re
import matplotlib.pyplot as plt

#TWITTER CLIENT
class TwitterClient():
	def __init__(self,twitter_user=None):
		self.auth=TwitterAuthenticator().authenticate_twitter_app()
		self.twitter_client=API(self.auth)
  
		self.twitter_user=twitter_user

	def get_twitter_client_api(self):
		return self.twitter_client

	def get_user_timeline_tweets(self,num_tweets):
		tweets=[]
		for tweet in Cursor(self.twitter_client.user_timeline,id=self.twitter_user).items(num_tweets):
			tweets.append(tweet)
		return tweets	

	def get_friend_list(self,num_friends):
		friend_list=[]
		for friend in Cursor(self.twitter_client.friends,id=self.twitter_user).items(num_friends):
			friend_list.append(friend)
		return friend_list

	def get_home_timeline_tweets(self,num_tweets):
		home_timeline_tweets=[]
		for tweet in Cursor(self.twitter_client.home_timeline,id=self.twitter_user).items(num_tweets):
			home_timeline_tweets.append(tweet)
		return home_timeline_tweets

#TWITTER AUTHENTICATOR
class TwitterAuthenticator():

	def authenticate_twitter_app(self):
		auth=OAuthHandler(twitter_credentials.CONSUMER_KEY,twitter_credentials.CONSUMER_SECRET)
		auth.set_access_token(twitter_credentials.ACCESS_TOKEN,twitter_credentials.ACCESS_TOKEN_SECRET)#method available in oauthhndlr
		return auth

class TwitterStreamer():#class for streaming and processing live tweets.

	def __init__(self):
		self.twitter_authenticator=TwitterAuthenticator()

	def stream_tweets(self,fetched_tweets_filename,hash_tag_list):#handles twitter authentication and connection to he twitter streaming api

		listener=TwitterListener(fetched_tweets_filename)
		auth=self.twitter_authenticator.authenticate_twitter_app()
		stream=Stream(auth, listener)
		stream.filter(track=hash_tag_list)#keywords for filtering

class TwitterListener(StreamListener):#class for printing recieved tweets to stdout

	def __init__(self,fetched_tweets_filename):
		self.fetched_tweets_filename=fetched_tweets_filename

	def on_data(self, data):
		try:
			print(data)
			with open(self.fetched_tweets_filename,'a') as tf:
				tf.write(data)
			return True
		except BaseException as e:
			print('Error on data: ')
		return True	

	def on_error(self, status):
		if status==420:#returning false in case rate limit occurs
			return False
		print(status)

class TweetAnalyzer():
	"""
	Functionality for analyzing and categorizing contents of tweets.
	"""
	def clean_tweet(self,tweet):
		return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",tweet).split())

	def analyze_sentiment(self,tweet):
		analysis=TextBlob(self.clean_tweet(tweet))

		if analysis.sentiment.polarity>0:
			return 1
		elif analysis.sentiment.polarity==0:
			return 0
		else:
			return -1

	def tweets_to_data_frame(self,tweets):
		df=pd.DataFrame(data=[tweet.text for tweet in tweets],columns=['tweets'])

		df['id']=np.array([tweet.id for tweet in tweets])
		df['len']=np.array([len(tweet.text) for tweet in tweets])
		df['date']=np.array([tweet.created_at for tweet in tweets])
		df['source']=np.array([tweet.source for tweet in tweets])
		df['likes']=np.array([tweet.favorite_count for tweet in tweets])
		df['retweets']=np.array([tweet.retweet_count for tweet in tweets])

		return df

if __name__=="__main__":

	twitter_client=TwitterClient()
	tweet_analyzer=TweetAnalyzer()
	api=twitter_client.get_twitter_client_api()

	tweets=api.user_timeline(screen_name='sundarpichai',count=200)  #inbuilt function from the twitter client
	#print(dir(tweets[0]))
	#print(tweets[0].id)
	#print(tweets[0].retweet_count)
	df=tweet_analyzer.tweets_to_data_frame(tweets)
	df['sentiment']=np.array([tweet_analyzer.analyze_sentiment(tweet) for tweet in df['tweets']])
	"""
	#print(df.head(10))
	#get average length over all tweets
	print(np.mean(df['len']))
	#get the number of likes for most liked tweets
	print(np.max(df['likes']))
	#time series
	time_likes=pd.Series(data=df['likes'].values,index=df['date'])
	time_likes.plot(figsize=(16,4),label='likes',legend=True)
	#plt.show()
	time_retweets=pd.Series(data=df['retweets'].values,index=df['date'])
	time_retweets.plot(figsize=(16,4),label='retweets',legend=True)
	plt.show()
	"""
	print(df.head(10))


