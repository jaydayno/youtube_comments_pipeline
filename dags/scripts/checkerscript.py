# %%
# Imports for extractions
import pandas as pd
import requests
import json
from datetime import datetime
import datetime

# Import for private data
import pathlib
from dotenv import dotenv_values

# Imports for writing data into a temp file
import csv
from tempfile import NamedTemporaryFile
import logging
from extract_spotify import extract_spotifyAPI


# %%
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")


# %%
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception(f"At least one of the returned songs ({timestamp}) does not have a yesterday's timestamp")

    return True



# %%
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

data = r.json()
song_names = []
artist_names = []
played_at_list = []
timestamps = []

for song in data["items"]:
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

song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
timestamplist = song_df["timestamp"].tolist()
for i, oneTimestamp in enumerate(timestamplist):
    stringtodatetime = datetime.datetime.strptime(oneTimestamp, '%Y-%m-%d')
    if stringtodatetime.day != yesterday.day:
        song_df = song_df.drop(index=i)
song_df.reset_index(inplace = True, drop = True)


if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")


with NamedTemporaryFile(mode='w', suffix='.csv') as temp:
    temp_path = str(pathlib.Path(__file__).parent.resolve()) + temp.name
    print(temp_path)
    temp.close()

# %%
