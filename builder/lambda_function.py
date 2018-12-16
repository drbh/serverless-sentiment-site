import json
import os, boto3, json
from boto3.dynamodb.conditions import Key, Attr
import datetime
from datetime import date, timedelta
from botocore.vendored import requests
from decimal import Decimal
from botocore.errorfactory import ClientError
import json, re, os
import random
from six.moves.html_parser import HTMLParser

h = HTMLParser()

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('tweet-sentiment')

import boto3

s3 = boto3.resource('s3')
BUCKET = "input.myexamplehugosite"
# s3.Bucket(BUCKET).upload_file("your/local/file", s3fname)

items=table.scan()["Items"]
# itm = items[0]

def build_article(title,content,date,publishdate,image,
	tags,comments,positive_sentiment,netural_sentiment,negative_sentiment,thresh):

	a =  ''.join([3*'-'])+'\n'\
	+ 'title: ' + '"' + title + '" \n'\
	+ 'date: ' + date +'\n'\
	+ 'publishdate: ' + publishdate +'\n'\
	+ 'image: ' + '"' + image+ '" \n'\
	+ 'tags: ' + '["' + '","'.join(tags) + '"] \n'\
	+ 'comments: ' + ("true" if comments else "false") +' \n'

	if positive_sentiment > thresh: 
		a = a + 'positive_sentiment: ' + '"'+str(positive_sentiment)+'" \n'
	if netural_sentiment > thresh: 
		a = a + 'netural_sentiment: ' + '"'+str(netural_sentiment)+'" \n'
	if negative_sentiment > thresh: 
		a = a + 'negative_sentiment: ' + '"'+str(negative_sentiment)+'" \n'

	a = a +''.join([3*'-'])+'\n'\
	+ '# '+ title + '\n'\
	+ content
	a = a.encode('ascii', 'ignore').decode('ascii')
	nm = title.encode('ascii', 'ignore').decode('ascii').strip().replace(" ","").lower()[0:5]

	os.chdir('/tmp')
	with open("articles/"+nm+".md", "w") as text_file:
		text_file.write(a)

	fname="file"
	s3fname="content/blog/"+nm+".md"
	localfname="articles/"+nm+".md"
	s3.Bucket(BUCKET).upload_file(localfname, s3fname)

	return a

def lambda_handler(event, context):
	netural_sentiment = 0
	negative_sentiment = 0
	thresh = 0
	
	for itm in items:
		title = itm["original"]["title"]
		content = h.unescape(itm["original"]["description"]) 
		date = itm["original"]["pubDate"]
		date = itm["original"]["pubDate"]
		publishdate = itm["original"]["pubDate"]
		image = ""
		tags = [itm["original"]["source"]]
		comments = False
		positive_sentiment = random.randint(0,10)
		article = build_article(title,content,date,publishdate,image,
		tags,comments,positive_sentiment,netural_sentiment,negative_sentiment,thresh)
		print article

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}
