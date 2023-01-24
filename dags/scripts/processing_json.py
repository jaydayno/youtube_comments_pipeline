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
        return Exception("No comments were extracted")

    # Primary Key Check
    if df['id'].is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls on id
    if df['id'].isnull().values.any():
        raise Exception("Null values found")
        
    return True

def clean_data(data: dict) -> pd.DataFrame:
    # works if data is dict
    list_of_values = []
    for key in data:
        list_of_values.append(data[key])
    df = pd.DataFrame(list_of_values, index= ["id", "author_channel_id", "author", "viewer_rating", "published_at", "updated_at", "display_text"]).transpose()
    
    df['published_at'] = df['published_at'].apply(lambda x: 
        datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S"))

    df['updated_at'] = df['updated_at'].apply(lambda x: 
        datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S"))

    df = df.reset_index(drop = True)

    row_count = df['id'].size

    if check_if_valid_data(df):
        logging.info(f"Data valid, proceed to Load stage. Row count: {row_count}")
    
    return df
    
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