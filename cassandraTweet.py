import time
import requests, json
import re
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('sahamtweet')

def addToTweets(id, hashtag, content, senderName, sendTime, sendTimePersian):
    session.execute(
    """
    INSERT INTO tweets (id, hashtag, content, senderName, sendTime, sendTimePersian)
    VALUES (%s, %s, %s, %s, %s, %s)
    """,
    (id, hashtag, content,senderName, sendTime, sendTimePersian)
)

def addToTweetsBySender(id, content, senderName, sendTimePersian):
    session.execute(
    """
    INSERT INTO tweets_by_sender (id, content, senderName, sendTimePersian)
    VALUES (%s, %s, %s, %s)
    """,
    (id, content, senderName, sendTimePersian)
)

def addToTweetsByHashtag(id, content, hashtag, sendTimePersian):
    session.execute(
    """
    INSERT INTO tweets_by_hashtag (id, hashtag, content, sendTimePersian)
    VALUES (%s, %s, %s, %s)
    """,
    (id, hashtag, content, sendTimePersian)
)

url = 'https://www.sahamyab.com/guest/twiter/list?v=0.1'
count_needed, sleep_time = 400, 5

current_count = 0

while current_count < count_needed:
    response = requests.request('GET', url, headers={'User-Agent': 'Chrome/61'})
    result = response.status_code
    if result == requests.codes.ok:
        data = response.json()['items']
        for d in data:
            try:
                if (d['content']):
                    id = int(d['id'])
                    hashtag = re.findall(r"#(\w+)", d['content'])[0]
                    content = d['content']
                    senderName = d['senderName']
                    sendTime = d['sendTime']
                    sendTimePersian = d['sendTimePersian'].replace('/', '-')
                    addToTweets(id, hashtag, content, senderName, sendTime, sendTimePersian)
                    addToTweetsBySender(id, content, senderName, sendTimePersian)
                    addToTweetsByHashtag(id, content, hashtag, sendTimePersian)
            except Exception as e:
                print("Upsert exception: " + str(e))
        current_count += 2
    else:
        print("Response code error: " + str(result))
    time.sleep(sleep_time)



