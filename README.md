Build a data pipeline to extract tweets mentioning given word and store them 
in tables that can be queried. Data is ingested from Twitter API, then moved
to S3 bucket, and then transformed into tables in AWS Postgres.

![](https://github.com/padekdee/Twitter-Data-Analysis/blob/main/Architecture.PNG)

Prerequisite:
1. AWS access keys, secret key
2. Twitter API Key and Secret, Access Token and Secret
3. s3 bucket created with name twitterData

Processing Logic:
1. Extract tweets with given keywords and possible variations by using 
   Twitter API
2. Extract id, date created, text, retweet and hashtag from tweets.
3. Save this data into .json in s3 location.
4. Read data from view created in PostgreSQL.

Extending Project ideas:
1. Create data model with dimension tables like user, location, date etc.
2. Write logic to populate dimensions and fact table.
3. Create AWS Step functions to build EMR with given configuration, IAM role and run
   spark job.
