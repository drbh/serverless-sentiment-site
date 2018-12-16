import os, boto3, json
from boto3.dynamodb.conditions import Key, Attr
import datetime
from datetime import date, timedelta
from botocore.vendored import requests
from decimal import Decimal
from botocore.errorfactory import ClientError
import requests, json, re

    
client = boto3.client('comprehend')

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('tweet-sentiment')
print("Connected to table")

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_data(symbol):
    abc = 'https://news.google.com/news/rss/search/section/q/'+symbol+'/'+symbol+'?hl=en&gl=US&ned=us'
    xyz = requests.get(abc)
    arts = xyz.content.replace("<\/item>", "").split("<item>")
    print("Downloaded articles from Google about %s" % symbol)
    return arts  

def get_title(a):
    return re.search(r'.*?\<title\>(.*)\<\/title\>.*', a).group(1)

def get_link(a):
    return re.search(r'.*?\<link\>(.*)\<\/link\>.*', a).group(1)

def get_guid(a):
    return re.search(r'.*?\<guid.*\>(.*)\<\/guid\>.*', a).group(1)
    
def get_pubDate(a):
    return re.search(r'.*?\<pubDate\>(.*)\<\/pubDate\>.*', a).group(1)

def get_description(a):
    return re.search(r'.*?\<description\>(.*)\<\/description\>.*', a).group(1)

def get_source(a):
    return re.search(r'.*?\<source.*\>(.*)\<\/source\>.*', a).group(1)

def parse_articles(arts):
    keys = []
    newer = set()

    to_update_articles = {}
    for a in arts[95:]:
        # pat = r'(?<=\>).+?(?=\<)'
        # x = re.findall(pat, a)
        title = get_title(a)
        link = get_link(a)
        guid = get_guid(a)
        pubDate = get_pubDate(a)
        description = get_description(a)
        source = get_source(a)
        obj = {
            "title": title,
            "link": link,
            "guid": guid,
            "pubDate": pubDate,
            "description": description,
            "source": source
        }
        keys += [{
            'tweetid': guid
        }]
        newer.add(guid)
        to_update_articles[guid] = obj
        # vvv+=[obj]

    print("Read and parsed articles")
    return keys, newer, to_update_articles

def compare(keys):
    already_have = dynamodb.batch_get_item(
        RequestItems={
            'tweet-sentiment': {
                "AttributesToGet": [ "tweetid" ],
                'Keys': keys,            
                'ConsistentRead': True            
            }
        },
        ReturnConsumedCapacity='TOTAL'
    )['Responses']['tweet-sentiment']
    have = set()
    if len(already_have) < 1:
        pass
    else:   
        for ah in already_have:
            tid = ah['tweetid']
            have.add(tid)
    print("Check database for exisiting copies")
    return have


def fetch_parse_analyze_write(sym):
    articles = get_data(sym)
    keys, newer, to_update_articles = parse_articles(articles)
    have = compare(keys)
    should_update =  list(newer - have)
    print("Compare local and exisiting")
    print("%s articles that need to be updated" % len(should_update))
    print("Running logic and writing to db")
    for su in should_update:
        item = to_update_articles[su]

        sentiment=client.detect_sentiment(Text=item['title'],LanguageCode='en')#['Sentiment']
        # have to make floats Decimals
        sentiment['SentimentScore']['Positive'] = Decimal(str(sentiment['SentimentScore']['Positive']))
        sentiment['SentimentScore']['Negative'] = Decimal(str(sentiment['SentimentScore']['Negative']))
        sentiment['SentimentScore']['Neutral'] = Decimal(str(sentiment['SentimentScore']['Neutral']))
        sentiment['SentimentScore']['Mixed'] = Decimal(str(sentiment['SentimentScore']['Mixed']))

        try:
            rsp = table.put_item(Item={"tweetid":item['guid'],"sentiment": sentiment, "original": item })
            # rsp = table.put_item(Item={"tweetid": item['guid'], "original": item['title']}, ConditionExpression='attribute_not_exists(tweetid)')
            # print rsp
        except ClientError as e:
            print("passed")
            pass

def lambda_handler(event, context):
    fetch_parse_analyze_write("Tesla")
    result = {"action":"updated"}
    return {
        'statusCode': 200,
        'body': json.dumps(result, default=decimal_default)
    }





