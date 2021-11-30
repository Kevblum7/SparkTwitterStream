# SparkTwitterStream

Introduction to Big Data Project

Do twitter streaming analysis using Spark streaming
Train machine learning algorithms to perform sentiment analysis based on the training data and classify stream of tweets using Spark MLib
Store the fetched tweets data and their classification results in a database



Training data could be obtained from here:

https://docs.google.com/file/d/0B04GJPshIjmPRnZManQwWEdTZjg/edit



With the following format:

The data is a CSV with emoticons removed. Data file format has 6 fields:

0 - the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)

1 - the id of the tweet

2 - the date of the tweet

3 - the query. If there is no query, then this value is NO_QUERY.

4 - the user that tweeted

5 - the text of the tweet
