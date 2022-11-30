import pandas as pd
import json
import requests
from to_db import main


def vid_info(pages, domain, api, channel_id):
    nextpagetoken = ''
    df = []

    for _ in range(pages):
        channel_url = domain + f'search?key={api}&part=snippet&channelId={channel_id}&maxResults=20&pageToken={nextpagetoken}'
        data = json.loads(requests.get(channel_url).text)

        for item in data['items']:
            stats = {}
            if item['id']['kind'] == 'youtube#video':
                video_id = item['id']['videoId']
                video_title = item['snippet']['title']
                publish_date = item['snippet']['publishedAt']
                publish_date = str(publish_date).split('T')[0]

                stats['video_id'] = video_id
                stats['video_title'] = video_title
                stats['published_date'] = publish_date

                vid_url = domain + f'videos?id={video_id}&part=statistics&key={api}'

                response_vid_stats = json.loads(requests.get(vid_url).text)

                stats['view_count'] = response_vid_stats['items'][0]['statistics']['viewCount']
                stats['like_count'] = response_vid_stats['items'][0]['statistics']['likeCount']
                stats['comment_count'] = response_vid_stats['items'][0]['statistics']['commentCount']
                df.append(stats)
                
        nextpagetoken = data['nextPageToken']

    df = pd.DataFrame(df)
    return df



if __name__ == '__main__':
    api = 'your_api_here'
    channel_id = 'UCXuqSBlHAE6Xw-yeJA0Tunw' # LinusTechTip
    n_pages = 10

    domain = f'https://www.googleapis.com/youtube/v3/'
    
    df = vid_info(n_pages, domain, api, channel_id)
    # df.to_csv('data.csv', index=False)
    # df1 = pd.read_csv('data.csv')

    main(df)

