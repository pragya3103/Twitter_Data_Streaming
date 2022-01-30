#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 29 15:04:38 2021

@author: rita
"""
import tweepy
import time
from kafka import KafkaProducer
import json
from twitter_keys_xml_parser import getData

# Twitter API's Key

access_token = getData("access_token")
access_token_secret = getData("access_token_secret")
consumer_key = getData("consumer_key")
consumer_secret = getData("consumer_secret")

# Creating authentication object

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# Setting your access token and secret

auth.set_access_token(access_token, access_token_secret)

# Creating the API object by passing in auth information

api = tweepy.API(auth)

# Formatting date and time

from datetime import datetime

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return mytime.strftime("%Y-%m-%d %H:%M:%S")

# KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,11,5))
topic_name = 'trump'

# Get data from Twitter

def get_twitter_data():
    res = api.search_tweets('#omicron')
    for i in res:
        date = str(i.created_at)
        
        hashtags = i.entities['hashtags']
        hashtext = list()
        for j in range(0, len(hashtags)):
            hashtext.append(hashtags[j]['text'])
        
        record = dict()
        record['Created_date']=str(date[:16])
        record['Username']=str(i.user.screen_name)
        record['User_location']=str(i.user.location)
        record['Hashtags']=str(hashtext)
        record['Text']=str(i.text[:100])
        
        producer.send(topic_name, json.dumps(record, ).encode('utf-8'))
        

def periodic_work(interval):
    while True:
        get_twitter_data()
        # interval should be an integer, the number of seconds to wait
        time.sleep(interval)
        
periodic_work(60*0.1) # get data every couple of minutes

