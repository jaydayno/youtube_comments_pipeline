# %%
import requests
from urllib.parse import urlparse
from dotenv import dotenv_values
from pathlib import Path
from bs4 import BeautifulSoup

def parse_channel_id(url_string):
    """
    Examples:
    - https://www.youtube.com/@UnderTheInfluenceShow
    - https://www.youtube.com/channel/UC2xskkQVFEpLcGFnNSLQY0A
    """
    par_result = urlparse(url_string)
    if par_result.path.startswith('/@'):
        page = requests.get(url_string)
        soup = BeautifulSoup(page.content, 'html.parser')
        html = list(soup.children)[1]
        tag = html.find(True, {'rel': 'canonical'})
        href_link = tag['href']
        return parse_channel_id(href_link)
    elif par_result.path.startswith('/channel'):
        return par_result.path.split('/')[2]
    return None

path_lib = Path(__file__).parent.resolve()
config = dotenv_values(f"{path_lib}/configuration.env")
# %%
# Provide Youtube Link
channel_link = 'https://www.youtube.com/@UnderTheInfluenceShow'

channel_id = parse_channel_id(channel_link)

params = {
    "key" : config['youtube_api_key'],
    "textFormat" : "HTML",
    "part" : "snippet",
    "allThreadsRelatedToChannelId" : channel_id,
    "maxResults" : 5,
}
# Add &pageToken={nextPageToken} for multiple comments on a video

r = requests.get("https://www.googleapis.com/youtube/v3/commentThreads", params = params)

data = r.json()

comment_ids, comment_authors, comment_authors_channelIds, comment_publishedAt_dates, comment_updatedAt_dates, comment_viewerRatings, comment_displayTexts = ([] for i in range(7))

for item in data['items']:
    comment_ids.append(item['id'])
    comment_authors_channelIds.append(['snippet']['topLevelComment']['snippet']['authorChannelId']['value'])
    comment_authors.append(item['snippet']['topLevelComment']['snippet']['authorDisplayName'])
    comment_viewerRatings.append(item['snippet']['topLevelComment']['snippet']['viewerRating'])
    comment_publishedAt_dates.append(item['snippet']['topLevelComment']['snippet']['publishedAt'])
    comment_updatedAt_dates.append(item['snippet']['topLevelComment']['snippet']['updatedAt'])
    comment_displayTexts.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])