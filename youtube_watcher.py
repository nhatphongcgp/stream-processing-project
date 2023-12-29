import logging
import sys
import requests
from config import config
import json
from pprint import pformat
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer


def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/playlistItems",
        params={
            "key": google_api_key,
            "playlistId": youtube_playlist_id,
            "part": "contentDetails",
            "pageToken": page_token,
        },
    )

    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload


def fetch_videos_page(google_api_key, video_id, page_token=None):
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        params={
            "key": google_api_key,
            "id": video_id,
            "part": "snippet,statistics",
            "pageToken": page_token,
        },
    )

    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload


def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):
    """Generator function that yields video items from a specified YouTube playlist.
    This function uses recursion to fetch all videos from a YouTube playlist.
    It retrieves one page of videos at a time and yields the items from each page.
    If there are more pages available, it will continue to fetch and yield items until all pages have been retrieved.

    Usage: # Create a generator items_generator = fetch_playlist_items('YOUR_GOOGLE_API_KEY', 'PLAYLIST_ID')
    # Iterate through the generator to get playlist items
    # for item in items_generator:
    #    print(item)
    # Or process the item as needed
    # Note: Each video item yielded by this function is a dictionary with details about the video.
    # The nextPageToken is used internally to manage pagination and is not required for initial calls.
    # """

    """
        Yields ==> dict[key]: return value of a dictionary.The value is representing a single video item or many video items in the playlist.
    """
    payload = fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token)
    
    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_playlist_items(
            google_api_key, youtube_playlist_id, next_page_token
        )

def fetch_videos(google_api_key, youtube_playlist_id, page_token=None):
    payload = fetch_videos_page(google_api_key, youtube_playlist_id, page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_videos(google_api_key, youtube_playlist_id, next_page_token)


def summarize_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likeCount", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }


def on_delivery(err, record):
    pass


def main():
    logging.info("START")

    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_value_schema = schema_registry_client.get_latest_version(
        "youtube_videos-value"
    )

    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_value_schema.schema.schema_str,
        ),
    }
    producer = SerializingProducer(kafka_config)

    google_api_key = config["google_api_key"]
    youtube_playlist_id = config["youtube_playlist_id"]

    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):
            logging.info("GOT %s", pformat(summarize_video(video)))

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

    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
