# BigData
Projects for Big Data course, part of the Master studies.

## Setup
All 3 projects were developed using docker and docker compose.  

Big Data Europe (https://hub.docker.com/u/bde2020) containers were used for hadoop and spark instances and also for application deployment container.
For kafka and zookeeper instances wurstmeister (https://hub.docker.com/u/wurstmeister) containers were used and for Cassandra official docker image was used.

## Setup - Docker-compose files
  
- docker-compose.yaml - contains configuration for hadoop nodes/components containers and spark master and consumer containers, run this docker-compose file before any other
- docker-compose-submit.yaml - runs deploy container for spark submit application
- docker-compose-streaming.yaml - runs zookeeper, kafka and cassandra containers, and also containers for spark streaming producer and spark streaming consumer applications
- docker-compose-model-training.yaml - runs container for spark application for training supervised machine learning model
- docker-compose-stream-classification.yaml - runs zookeeper and kafka containers, and also spark stream producer and spark classification consumer applications

## Dataset

Traffic accidents dataset was used which can be found on the following link (https://smoosavi.org/datasets/us_accidents).  

Dataset contains around around 3 million traffic accidents, which covers 49 states of the contiguous United States. The data is continuously being collected from February 2016, using several data providers, including multiple APIs that provide streaming traffic event data.

Dataset contains 47 attributes denoting locations of accidents, duration of accidents, attributes describing location where accident has happen, weather information approximately in the time of event, notable Points of Interest. 

Before deploying application (running deploy container with docker compose), run upload-dataset-to-hdfs shell script to deploy dataset from namenode container to HDFS (this shell script executes commands in namenode container to deploy dataset to HDFS).

## Project 1 - Submit app
Project 1 demonstrates the usage of spark batch processing on Traffic Accidents dataset. 
Following calculations have been performed: 
    -stopVsGiveWay - compares number of accidents on Give_Way and Stop signs in a given city for a given time interval
    -junctionComparison - compares number of accidents on Roundabouts and TrafficSignals in a given city for a given time interval
    -distanceAndDurationStatsForState - calculates distance and shows statistics for distance and duration of accidents for a given State in a given time interval
    -cityWithMaxAccidentsInPeiod - calculates most dangerous city per state for a time interval
    -cityAccidentsPOI - number of accidents per Point of Interest for a given city in a time interval
    -weatherConditionsDuringAcciedents - calculates most dangerous weather condition in a given city for a time interval
    -weatherConditionStatisticsForStreets - weather statistics per street for a given city in a given time interval


## Project 2 - Spark stream processing

Project 2 consists of 2 applications: **stream-producer** and **stream-consumer**.  
**stream-producer** reads one line at a time from dataset (dataset is saved on HDFS) and publishes it to Kafka **accidents** topic.  
**stream-consumer** reads published data from **accidents** topic and finds
- City with most accident in given time window and saves that value in Cassandra cities_with_most_accidents table
- Maximum and minimum of accident duration, average duration of accident and accident count for given city and time window, results are saved in Cassandra duration_statistic table
- Counts accidents on jucntions, traffic signals, and roundabouts in a given time window. Statistics are then calculated and stored in Cassandra poi_statistics table 

## Project 3 - Spark ML

Project 3 also consists of 2 applications: batch-model-training and stream-classification  

**batch-model-training** batch-model-training is app for training ML model for predicting severity of accident (classification) depending on accident location, weather condition during accident, distance of accident and description. Two models were tested - nlp logistic regression model based on description and random forest model. Models are saved on HDFS along with pipeline objects that preprocess data before feeding it the model

**stream-classification** is application that loads model and preprocessing objects from HDFS and feeds published data from **accidents** Kafka stream to the model and displays the results.


## Literature
[1] Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy, and Rajiv Ramnath. “A Countrywide Traffic Accident Dataset.”, arXiv preprint arXiv:1906.05409 (2019).
[2] Literatura sa predavanja
[3] Marz, and Warre. "Big Data:Principles and best practices of scalable realtime data systems", Manning Publications Co.(April 2015)
[4] Holden, Andy, Patrick, and Matei Zaharia. "Learning Spark", O’Reilly Media, Inc (2015)