## Introduction

This readme file describes how to use a Kafka Cluster and Confluent to track comments or likes on someone else’s YouTube video and send it to a telegram bot

## Prerequisites
Because this project using Python language (3.11.3) so all we need is:
* confluent-kafka==2.2.0
* fastavro==1.8.2
* python-dateutil==2.8.2
* requests==2.31.0

## Architecture
The architecture for this solution is as follows:
    First of all, we need to get a list video from JSON 
    response YouTube Data API.
    The list video was produced to a Kafka topic.
    The Kafka cluster stores the messages (list video).
    At this step with perform SQL to process message. After that,
    a consumer application is a telegram bot reads the messages
    from the Kafka topic and takes action sending a notification.

## Steps

 * ### Step 1:

    Getting a List of Video Responses from the YouTube API

    After getting list we have "model" of a video included:   video_id, title, views, likes, comments,

    Note: You will need a API key to perform request
 * ### Step 2:

    1: Create a enviroment and kSQL cluster in Confluent

    2: Write a querry to create a stream and infer to a schema of messages we need perform (By using avro format we don't need to create a specific schema for that). We using keyword WITH because we want to create a stream underlying a specific topic we want to store it

    ```bash
    CREATE STREAM youtube_videos (
        video_id VARCHAR KEY,
        title VARCHAR,
        views INTEGER,
        comments INTEGER,
        likes INTEGER
    ) WITH (
        KAFKA_TOPIC = 'youtube_videos',
        PARTITIONS = 1,
        VALUE_FORMAT = 'avro'
    )
    ```
 * ### Step 3:

    We produce a message to youtube_videos topic in python code

    ```bash
                producer.produce(
                topic="youtube_videos",
                key=video_id,
                value={
                    "TITLE": video["snippet"]["title"],
                    "VIEWS": int(video["statistics"].get("viewCount", 0)),
                    "LIKES": int(video["statistics"].get("likeCount", 0)),
                    "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                },
                on_delivery=on_delivery,
            )
    ```

* ### Step 4:
    Check this youtube_videos stream by using query below
    ```bash
        SELECT *
        FROM youtube_videos
        EMIT CHANGES;
    ```
    <img width="400" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/64cde333-faa3-408c-a565-f4179c4f21f1">


* ### Step 5:
    Create a table to "watch" if someone "changed" a video in   youtube_videos stream
    ```bash
    CREATE TABLE youtube_changes WITH (KAFKA_TOPIC='youtube_changes') AS SELECT
        video_id,
        latest_by_offset(title) AS title,
        latest_by_offset(comments, 2)[1] AS comments_previous,
        latest_by_offset(comments, 2)[2] AS comments_current,
        latest_by_offset(views, 2)[1] AS views_previous,
        latest_by_offset(views, 2)[2] AS views_current,
        latest_by_offset(likes, 2)[1] AS likes_previous,
        latest_by_offset(likes, 2)[2] AS likes_current
    FROM youtube_videos
    GROUP BY video_id
    EMIT CHANGES;
    ```
    In this code above, we using latest_by_offset aggregate function in KSQL returns the latest value and we add two parameters. One for column name and one for 2 lasted values of this column
* ### Step 6:
    Check the changes of a example video like liking a video of the list of videos then we produce again. After that we query this code bellow
  ```bash
  SELECT * FROM YOUTUBE_CHANGES
  WHERE likes_previous <> likes_current
  EMIT CHANGES;
  ```
  <img width="400" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/e64731b6-8c1d-4e5a-abc8-a71d6938e412">

    
* ### Step 7:
    1: Create a telegram bot and get chat id

    2: We need to identify a json payload to send messages.

    3: At this https://core.telegram.org/bots/api#sendmessage. We have 2 required parameters: chat_id and text to send a message
* ### Step 8:
    Create a stream to store all messages that ought to go to telegram outbox
    ```bash
    CREATE STREAM telegram_outbox (
        `chat_id` VARCHAR,
        `text` VARCHAR,
    ) WITH (
        KAFKA_TOPIC = 'telegram_outbox',
        PARTITIONS = 1,
        VALUE_FORMAT = 'avro'
    );
    ```
    Default of query will create a column with uppercase name. That case will not match the json payload that Telegram are expecting.So we must to force this to prime case is chat_id and text

    That query code above like a POST request method 
* ### Step 9:
    1: Create a connector HTTP Sink

    2: Configure connector
  
  <img width="400" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/a470c719-bf8b-4879-94b2-145efca74fc1">
  
  <img width="400" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/12ac09f8-d9b2-4a89-bd85-fb82146a30c4">

    We must set the batch max size is 1, it means that only one message will be sent in each batch. In other words, each message will be produced individually without being grouped with any other messages.
    And we don't want to put a message in a array so we need set Batch json as array is false

      
* ### Step 10 :
    Check the telegram_outbox stream by inserting some dummy data and run this query below

    ```bash
    INSERT INTO telegram_outbox(
        `chat_id`,
        `text`
    ) VALUES(
        '5789454749',
        'Nearly there'
    );
    ```
    Check our bot and we get the result
    
    <img width="491" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/955d6d0c-eef7-4b5a-8f36-79e00e76432f">

* ### Step 11:
    So in this step we need define a stream to listen youtube_changes table change over time. Just think it subcribe a table change
    ```bash
    CREATE STREAM youtube_changes_stream WITH (KAFKA_TOPIC='youtube_changes', VALUE_FORMAT='avro');
    ```
    This query above will create a stream and infer schema from youtube_changes table

* ### Step 12:
    Create a query insert to youtube_changes_stream and run it

    ```bash
        INSERT INTO telegram_outbox
        SELECT '<your chat id>' AS `chat_id`,
            CONCAT(
                'Likes changed: ',
                CAST(likes_previous AS STRING),
                ' => ',
                CAST(likes_current AS STRING),
                '. ',
                title
                 ) AS `text`
        FROM youtube_changes_stream
        WHERE likes_current <> likes_previous;
    ```
    This query code is not accutally insert data. It's will insert data when some video was liked

* ### Step 13:
    Let's like a video in a list we test. And produce again then we get the result 
    
  <img width="50" alt="image" src="https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/6fa33e3b-4729-433b-a876-30895ea84b5d">
## Final Result


https://github.com/nhatphongcgp/Kafka-With-Confluent/assets/60643737/4560bdab-244d-4566-91dd-931f43b35434


## Authors

- [Phong Nguyen](https://github.com/nhatphongcgp)
