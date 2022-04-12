# Twitter ETL Project
## Overview
This is my first ETL project which calculates the polarity of the tweet and subjectivity of the tweet. 
## Architecture
![image](https://user-images.githubusercontent.com/32141306/162854910-5a62bd33-b86c-45a9-b9e0-df12400528c8.png)
## Process
1. Streams Tweets from Twitter using keywords to filter tweets.
2 Use Kafka Producer to Quere all the Tweets.
3. Use Apache Spark to read the Queue from the topic and perform sentiment analysis on them. 
4. Writing the transformed data back to Kafka. 
5. Storing data to Mongo using Kafka Consumer.




