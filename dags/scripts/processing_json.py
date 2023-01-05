import pandas as pd
from datetime import datetime
import datetime
import logging
import urllib
import boto3
import json
from io import StringIO

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
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
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1, hours=5)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception(f"At least one of the returned songs ({timestamp}) does not have a yesterday's timestamp")

    return True

def clean_data(data):
    song_df = pd.DataFrame(data, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    
    song_df['played_at'] = song_df['played_at'].apply(lambda x: 
        datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S"))

    # Convert time to Unix timestamp in miliseconds
    todaydate = datetime.date.today()
    todaytime = datetime.datetime.min.time()
    today = datetime.datetime.combine(todaydate, todaytime)
    yesterday = today - datetime.timedelta(days=1)

    timestamplist = song_df["timestamp"].tolist()
    row_count = len(song_df["timestamp"].tolist())
    count_of_dropped = 0
    for i, oneTimestamp in enumerate(timestamplist):
        stringtodatetime = datetime.datetime.strptime(oneTimestamp, '%Y-%m-%d')
        if stringtodatetime.day != yesterday.day:
            count_of_dropped += 1
            song_df = song_df.drop(index=i)

    
    song_df.reset_index(inplace = True, drop = True)


    if check_if_valid_data(song_df):
        logging.info(f"Data valid, proceed to Load stage with {count_of_dropped} rows dropped out of {row_count}")
    
    return song_df
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        json_data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        dict_data = json.loads(json_data)
        df = clean_data(dict_data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        stage_file_name = key.split(sep='/')[1].split(sep='.')[0]
        key_for_stage = 'stage/' + stage_file_name + '.csv'
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key_for_stage)
        return True
    except Exception as e:
        print(e)
        raise e