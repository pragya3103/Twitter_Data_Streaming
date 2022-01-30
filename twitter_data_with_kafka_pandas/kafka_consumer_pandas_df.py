#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 12:14:34 2022

@author: rita
"""
#python kafka_twitter_data_producer.py

from kafka import KafkaConsumer
import pandas as pd
import time
from collections import Counter
import re


d ={}
l = []
omi_list = []
consumer = KafkaConsumer('trump')
omi_df = ""
#consumer = KafkaConsumer('trump',value_deserializer=lambda m: literal_eval(m.decode('utf8')))

def consumer_reader():
    for message in consumer:
        for i in message.value.decode('utf8').split('\n'):
            l.append(i.split(':'))
        for j in l:
            if len(j)>1:
                d[j[0]] = [j[1]]
        df = pd.DataFrame(d)
        
        #df['Hashtags'].to_csv('./output.csv',mode='a',header=False,index=False)

        if len(df['Hashtags'][0])>2 :
            
            if 'Omicron' in df['Hashtags'][0] or 'omicron' in df['Hashtags'][0]:
                for i in df['Hashtags'][0].split(","):
                    
                    if i == "":
                        pass
                    else:
                        omi_list.append(re.sub(r"[^a-zA-Z0-9]","",i.lower()))             
                    
        c = Counter(omi_list)
        count_dict=dict(c)
        count_dict_descending = dict(sorted(count_dict.items(), key = lambda kv:(kv[1], kv[0]),reverse=True))
        #print(count_dict_descending)
        first5pairs = {k: count_dict_descending[k] for k in list(count_dict_descending)[:5]}
        print(first5pairs)
        if len(first5pairs)>0:
            word_count = first5pairs.items()
            x, y = zip(*word_count)
            plt.plot(x, y,color='red', marker='o')
            plt.plot(x, y)
            plt.xlabel('Hashtags')
            plt.ylabel('Counts')
            plt.title('Trending Hashtags Counts')
            plt.show()
        else:
            continue

def periodic_work(interval):
    while True:
        consumer_reader()
        # interval should be an integer, the number of seconds to wait
        time.sleep(interval)
        
periodic_work(60*0.1)