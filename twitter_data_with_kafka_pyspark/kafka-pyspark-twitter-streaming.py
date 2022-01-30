#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 16:53:46 2022

@author: rita
"""

# python Kafka_producer_pyspark.py

from pyspark.sql.types import StructType, StringType, StructField, DateType, ArrayType, MapType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, substring
from pyspark.sql.functions import col, from_json
import time
import pandas as pd
import os
from collections import Counter
import re
import matplotlib.pyplot as plt, mpld3

kafka_topic_name = "trump"
kafka_bootstrap_servers = "localhost:9092"
nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
jsonOptions = {"timestampFormat": nestTimestampFormat}

def all_data(posts_df):
    col_stream = posts_df.writeStream.trigger(processingTime='5 seconds')\
    .outputMode('update')\
    .option("truncate", "false")\
    .option("checkpointLocation", ".checkpoint/col_stream_")\
    .format("console")\
    .start()
                    
    col_stream.awaitTermination(1)
    
def remove_old_file(path):
    for filename in os.listdir(path):
        if os.path.isfile(os.path.join(path, filename)):
            os.remove(os.path.join(path, filename))
            
def reading_and_plotting(path):       
    files = os.listdir(path)
    result =[]
    
    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(file,names=["Location","Hashtags","Time"])
            for hashtag in df["Hashtags"]:
                words = re.findall('[a-zA-Z0-9]+', hashtag)
                for word in words:
                    result.append(word.lower())
    
    c = Counter(result)
    count_dict=dict(c)
    count_dict_descending = dict(sorted(count_dict.items(), key = lambda kv:(kv[1], kv[0]),reverse=True))
    first5pairs = {k: count_dict_descending[k] for k in list(count_dict_descending)[:5]}
    if len(first5pairs)>0:
        word_count = first5pairs.items()
        x, y = zip(*word_count)
        plt.plot(x, y,color='red', marker='o')
        plt.plot(x, y)
        plt.xlabel('Hashtags')
        plt.ylabel('Counts')
        plt.title('Trending Hashtags Counts')
        plt.show()
        #mpld3.show()


def hashtags_globally(posts_df):

    try:

        hashtags_location = posts_df.select('parsed_value.User_location','parsed_value.Hashtags', substring('timestamp', 1,16).alias("timestamp")) \
            .filter(col("Hashtags") != "[]") \
            .filter(col("Hashtags").contains('Omicron' or 'omicron')) \
            .filter(col("User_location") != "")

        hashtags_write_location = hashtags_location.writeStream.trigger(processingTime="60 seconds")\
            .outputMode('append')\
            .option("truncate", "false")\
            .option("checkpointLocation", "check")\
            .format("csv")\
            .option("path","./data")\
            .start()
        
        os.chdir(os.getcwd()+"/data")
        path = os.getcwd()

        def periodic_work(interval):
            while True:
                reading_and_plotting(path)
                remove_old_file(path)
                hashtags_write_location.awaitTermination(1)
                # interval should be an integer, the number of seconds to wait
                time.sleep(interval)
                
        periodic_work(60*0.1)
            
        
    except Exception as e:
        print(e)


if __name__ == "__main__":

    sparkSession = SparkSession \
        .builder \
        .appName("TwitterStreamingAssignment") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType() \
        .add("Created_date", DateType()) \
        .add("Username", StringType()) \
        .add("User_location", StringType())\
        .add("Hashtags", StringType()) \
        .add("Text", StringType())

    posts_df = sparkSession.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trump") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()\
        .select(from_json(col("value").cast("string"), schema, jsonOptions).alias("parsed_value"), col('timestamp'))

    posts_df.printSchema()


    #all data

    #all_data(posts_df)

    hashtags_globally(posts_df)
