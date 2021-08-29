import time
import requests, json
import re
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('sahamtweet')

print("choose type (hashtag/sender): ")
type = input()
print("declare value: ")
value = input()
print("declare duration with two times (e.g.")
print("e.g. 1400-04-08 10:10")
print("     1400-04-09 17:16):")
start = input()
end = input()

query = ""
if (type == "sender") : 
    query = "SELECT content FROM tweets_by_sender WHERE senderName=%s AND sendTimePersian >= %s AND sendTimePersian <= %s"
elif (type == "hashtag") : 
    query = "SELECT content FROM tweets_by_hashtag WHERE senderName=%s AND sendTimePersian >= %s AND sendTimePersian <= %s"

future = session.execute_async(query, [value, start, end])

try:
    rows = future.result()
    for r in rows:
        print(r.content)
except ReadTimeout:
    print("Query timed out")