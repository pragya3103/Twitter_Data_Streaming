# Twitter_Data_Streaming
Twitter data streaming using Twitter APIs

## Use Case 1: Twitter Streaming with Kafka and Python Pandas
Streaming real-time Twitter Trending Hash-Tag data using twitter API, Kafka, Python.

## Prerequisite:
1. Python
2. Twitter Developer Account
3. Kafka

## Python Download and Installation:
https://www.python.org/downloads/

## Twitter Developer Account and Elevated access:
https://developer.twitter.com/en/portal/products/elevated

## Kafka Download and Installation from below link:
https://kafka.apache.org/quickstart

Steps to run the code successfully - 

1. Stop previous running zookeeper(if zookeeper stops aburptly when you try to start) -
```
sudo service zookeeper stop
```
2. Start the zookeeper - 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
3. Start the kafka server in new terminal -
```
bin/kafka-server-start.sh config/server.properties
```
4. Create topics -
```
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic trump --bootstrap-server localhost:9092
```
5. Run the python code in new terminal - 
```
python kafka_producer_pandas.py
```
7. Start kafka consumer in new terminal-
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trump --from-beginning
```
8. Run the kafka consumer and streaming code
```
python kafka_consumer_pandas_df.py
```


## Use Case 2: Twitter Streaming with Kafka and Pyspark
Streaming real-time Twitter data using twitter API, Kafka, Pyspark.

## Prerequisite:
1. Python
2. Twitter Developer Account
3. Kafka
4. Spark

## Python Download and Installation:
https://www.python.org/downloads/

## Twitter Developer Account and Elevated access:
https://developer.twitter.com/en/portal/products/elevated

## Kafka Download and Installation from below link:
https://kafka.apache.org/quickstart

## Spark Download
https://spark.apache.org/downloads.html

Steps to run the code successfully - 

1. Stop previous running zookeeper(if zookeeper stops aburptly when you try to start) -
```
sudo service zookeeper stop
```
2. Start the zookeeper - 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
3. Start the kafka server in new terminal -
```
bin/kafka-server-start.sh config/server.properties
```
4. Create topics -
```
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic trump --bootstrap-server localhost:9092
```
5. Run the pyspark producer code in new terminal - 
```
python Kafka_producer_pyspark.py
```
7. Start kafka consumer in new terminal-
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trump --from-beginning
```
8. Now run the pyspark streaming code
```
python kafka-pyspark-twitter-streaming.py
