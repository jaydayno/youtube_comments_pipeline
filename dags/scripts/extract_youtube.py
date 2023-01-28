from airflow.hooks.S3_hook import S3Hook
import logging
import json
from tempfile import NamedTemporaryFile
import requests
from urllib.parse import urlparse
from dotenv import dotenv_values
from pathlib import Path
from bs4 import BeautifulSoup
from textwrap import dedent

path_lib = Path(__file__).parent.resolve()
config = dotenv_values(f"{path_lib}/configuration.env")

def parse_channel_id(url_string: str) -> str:
    """
    Called in 'extract_youtube_api' function.
    Parsing Youtube channel link.

    Args:
        url_string: Provide a valid youtube channel link (see examples).
                Examples:
                - https://www.youtube.com/@RihannaVEVO
                - https://www.youtube.com/channel/UC2xskkQVFEpLcGFnNSLQY0A
                - https://www.youtube.com/user/rihannavevo

    Returns:
        Returns the channel id by parsing the youtube channel links.

    Raises:
        ValueError: Parser was not able to parse the given link.
    """
    par_result = urlparse(url_string)
    if par_result.path.startswith('/@') or par_result.path.startswith('/user'):
        page = requests.get(url_string)
        soup = BeautifulSoup(page.content, 'html.parser')
        html = list(soup.children)[1]
        tag = html.find(True, {'rel': 'canonical'})
        href_link = tag['href']
        return parse_channel_id(href_link)
    elif par_result.path.startswith('/channel'):
        return par_result.path.split('/')[2]
    raise ValueError('Could not get channel id from link provided.')

def extract_youtube_api(channel_link: str, num_of_comments: int) -> dict:
    """
    Called in 'upload_to_s3' function.
    Runs a GET request to Youtube API and formats the data into a Python dictionary.

    Args:
        channel_link: Retrieves the channel_link, provided from 'upload_to_s3' call.
        num_of_comments: The max amount of comments, provided from 'upload_to_s3' call.

    Returns:
        Python dictionary containg lists of Youtube Comment data from their API:
            "id", "authorChannelId", "authorDisplayName", "viewerRating", 
            "publishedAt", "updatedAt", "textOriginal"
    """
    channel_id = parse_channel_id(channel_link)

    params = {
        "key" : config['youtube_api_key'],
        "textFormat" : "HTML",
        "part" : "snippet",
        "allThreadsRelatedToChannelId" : channel_id,
        "maxResults" : num_of_comments,
    }
    # Add &pageToken={nextPageToken} for multiple comments on a video

    r = requests.get("https://www.googleapis.com/youtube/v3/commentThreads", params = params)

    data = r.json()
    comment_ids, comment_authors, comment_authors_channelIds, comment_publishedAt_dates, comment_updatedAt_dates, comment_viewerRatings, comment_textOriginal = ([] for i in range(7))

    for item in data['items']:
        comment_ids.append(item['id'])
        comment_authors_channelIds.append(item['snippet']['topLevelComment']['snippet']['authorChannelId']['value'])
        comment_authors.append(item['snippet']['topLevelComment']['snippet']['authorDisplayName'])
        comment_viewerRatings.append(item['snippet']['topLevelComment']['snippet']['viewerRating'])
        comment_publishedAt_dates.append(item['snippet']['topLevelComment']['snippet']['publishedAt'])
        comment_updatedAt_dates.append(item['snippet']['topLevelComment']['snippet']['updatedAt'])
        comment_textOriginal.append(item['snippet']['topLevelComment']['snippet']['textOriginal'])

    comment_dict = {
                "comment_ids" : comment_ids,
                "comment_authors_channelIds": comment_authors_channelIds,
                "comment_authors" : comment_authors,
                "comment_viewerRatings" : comment_viewerRatings,
                "comment_publishedAt_dates" : comment_publishedAt_dates,
                "comment_updatedAt_dates" : comment_updatedAt_dates,
                "comment_displayTexts" : comment_textOriginal
            }

    return comment_dict

def upload_to_S3(ti, target_name: str, channel_link: str, num_of_comments: int) -> bool:
    """
    Called in DAG.
    Uploads raw data (Python dictionary) in S3

    Args:
        ti: TaskInstance type from Airflow, specific variable for templating.
        target_name: Directory structure in S3, concluded with file name and type.
        channel_link: Provide a valid youtube channel link (see examples).
                        Examples:
                        - https://www.youtube.com/@UnderTheInfluenceShow
                        - https://www.youtube.com/channel/UC2xskkQVFEpLcGFnNSLQY0A
        num_of_comments: The max amount of comments needed.

    Returns:
        Returns True when upload was successful.
        Returns False when the target_name already exists (i.e. already uploaded in S3).
    """
    # Retrieves channel_name from GET request and pushes to xcoms 
    # (for tasks: 'create_channel_specific_sql_script')
    page = requests.get(channel_link)
    soup = BeautifulSoup(page.content, 'html.parser')
    html = list(soup.children)[1]
    tag = html.find(True, {'property': 'og:title'})
    channel_name = tag['content']
    ti.xcom_push(key='channel_name', value=channel_name)

    # Start Upload to S3
    BUCKET_NAME = config['bucket_name']
    s3 = S3Hook(aws_conn_id='aws_default')

    if s3.check_for_key(key=target_name, bucket_name=BUCKET_NAME) == False:
        try:
            data = extract_youtube_api(channel_link, num_of_comments)
            with NamedTemporaryFile('w+', encoding='utf-8') as f:
                json.dump(data, f)
                f.seek(0)
                s3.load_file(filename=f.name, bucket_name=BUCKET_NAME, key=target_name)
                logging.info(f"json from Youtube API uploaded to S3 bucket: {BUCKET_NAME} with name {target_name} with size {f.tell()}")
                return True
        except:
            raise
    else:
        logging.info(f"ALREADY UPLOADED to S3 bucket: {BUCKET_NAME} with name {target_name}")
        return False

def add_channel_sql(ti, channel_name: str) -> bool:
    """
    Called in DAG.
    Creating the CREATE sql script with channel name.

    Args:
        ti: TaskInstance type from Airflow, specific variable for templating.
        channel_name: Channel name provided by function 'parse_channel_id' via xcom.
                Examples:
                - https://www.youtube.com/channel/UC2xskkQVFEpLcGFnNSLQY0A --> RihannaVEVO

    Returns:
        Returns True if SQL query is created with channel name.

    Raises:
        ValueError: Could not create SQL query with channel_name, check path.
    """
    try:
        with open(f'dags/sql/youtube_{channel_name}_create.sql', 'w+') as fi:
            fi.seek(0)
            fi.truncate()
            fi.write(
            f"DROP TABLE IF EXISTS youtube_{channel_name}_data;" +
            dedent(f"""
            CREATE TABLE IF NOT EXISTS youtube_{channel_name}_data (
                id character varying,
                author_channel_id character varying,
                author character varying,
                viewer_rating character varying,
                published_at character varying,
                updated_at character varying,
                display_text character varying,
                text_polarities character varying,
                classifications character varying
            );
            """))
            val_table_name = f'youtube_{channel_name}_data'
            ti.xcom_push(key='whole_table_name', value=val_table_name)
            return True
    except:
        raise ValueError('Could not create SQL query.')
