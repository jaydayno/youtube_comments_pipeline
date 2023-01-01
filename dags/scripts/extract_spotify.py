# %%
from airflow.hooks.S3_hook import S3Hook
import requests
from tempfile import NamedTemporaryFile
from datetime import datetime
import datetime
import json
import logging

import pathlib
from dotenv import dotenv_values

# %%
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

# %%
def extract_spotify_API():
    TOKEN = config["TOKEN"]

    # headers needed for the GET request of Spotify API
    headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : f"Bearer {TOKEN}"
        }

    # Convert time to Unix timestamp in miliseconds
    todaydate = datetime.date.today()
    todaytime = datetime.datetime.min.time()
    today = datetime.datetime.combine(todaydate, todaytime)
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", headers = headers)

    json_data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in json_data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])


    song_dict = {
            "song_name" : song_names,
            "artist_name": artist_names,
            "played_at" : played_at_list,
            "timestamp" : timestamps
        }

    with NamedTemporaryFile('w+', encoding='utf-8') as f:
        json.dump(song_dict, f)
        logging.info(f"Current size of song_dict is {f.tell()}")

    return song_dict

def upload_to_S3(target_name: str, script_loc: str = None) -> False:
    BUCKET_NAME = config['bucket_name']

    s3 = S3Hook(aws_conn_id='aws_default')
    if script_loc == None:
        spot_data = extract_spotify_API()
        with NamedTemporaryFile('w+', encoding='utf-8') as f:
            json.dump(spot_data, f)
            s3.load_file(filename=f.name, bucket_name=BUCKET_NAME, key=target_name)
            logging.info(f"json from spotify API uploaded to S3 bucket: {BUCKET_NAME} with name {target_name}")
            return True
    else:
        s3.load_file(filename=script_loc, bucket_name=BUCKET_NAME, key=target_name)