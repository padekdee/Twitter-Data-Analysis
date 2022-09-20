use role sysadmin;
use warehouse tweet_wh;
use schema tweets_db.public

--s3 bucket for twitter data is twitterData

-- View in postgres

create table tweets_data(
 id primary key,
 created_date timestamp not null,
 text string not null,
 retweet string not null,
 hashtags string not null
);

select aws_s3.table_import_from_s3(
    'tweets_data',
    'id, created_date, text, favorite_count, retweet, hashtags',
    '(format json, header true)',
    'tweets_db.public',
    'twitterData'
)

--selecting data from view
select * from tweets_data limit 5;

