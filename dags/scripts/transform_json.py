from datetime import datetime
from io import StringIO
import pandas as pd
import datetime
import logging
import urllib
import boto3
import json


def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Called in transform_data() function.
    Retrieves the df from lambda_handler() >> transform_data() and checks if empty, no duplications and nulls

    Args:
        df: ***Validating*** the dataframe, making sure it follows proper ***cleaning, reshaping and de-duplication***

    Returns:
        Returns True when validation was successful.
    """
    # Check if dataframe is empty
    if df.empty:
        raise Exception("No comments were extracted")

    # Primary Key Check/duplicate check
    if df['id'].is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls on id
    if df['id'].isnull().values.any():
        raise Exception("Null values found")
        
    return True

def transform_data(data: dict) -> pd.DataFrame:
    """
    Called in lambda_handler() function.
    Retrieves the  from lambda_handler() >> transform_data() and checks if empty, no duplications and nulls
    
    Args:
        data: ***Formatting*** the data into tables or joined tables to match the schema of the target data warehouse/rds database.

    Returns:
        Returns a valid dataframe 
    """    
    
    list_of_values = []
    for key in data:
        if key == 'comment_displayTexts':
            list_of_values.append(
            [x.encode('utf-16', 'surrogatepass')
            .decode('utf-16')
            .encode("raw_unicode_escape")
            .decode("latin_1") for x in data[key]])
        else:
            list_of_values.append(data[key])
    df = pd.DataFrame(list_of_values, index= ["id", "author_channel_id", "author", "viewer_rating", "published_at", "updated_at", "display_text"]).transpose()
    
    df['published_at'].apply(lambda x: 
        datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S"))

    df['updated_at'] = df['updated_at'].apply(lambda x: 
        datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S"))

    df = df.reset_index(drop = True)

    row_count = df['id'].size

    if check_if_valid_data(df):
        logging.info(f"Data valid, proceed to Load stage. Row count: {row_count}")
    
    return df
    
def lambda_handler(event, context):
    """
    Invoked with event via Airflow.
    Locates the bucket with its name and extracts the raw data (json). 
    Loads into transform_data() as a python dictionary and uploads stage data into S3.
    
    Args:
        event: json (specified in invoke_with_operator() function from DAG), includes bucket_name and key of the raw data.
        context: provides properties/methods which hold info about the invocation, function, and execution environment.

    Returns:
        bool: True if the extraction, transformation/validation and re-upload worked. 
    """    
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        json_data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        dict_data = json.loads(json_data)
        df = transform_data(dict_data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        stage_file_name = key.split(sep='/')[1].split(sep='.')[0]
        key_for_stage = 'stage/' + stage_file_name + '.csv'
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key_for_stage)
        return True
    except Exception as e:
        print(e)
        raise e