import json
import oauth2 as oauth
import os
import re
import requests
import requests_oauthlib
import socket
import sys
# sys.path.insert(0, './services/')
import thread
import time
from datetime import datetime
from firebase import firebase
from pycorenlp import StanfordCoreNLP
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from spark_client import spark_client
from threading import Thread


'''
Credit for template:
    Analyze Social Media Data in Real Time -
        https://bigdatauniversity.com/blog/analyze-social-media-data-real-time
    Github -
        https://github.com/saeedaghabozorgi/TwitterSparkStreamClustering

Twitter Streaming API Request Parameters:
    https://dev.twitter.com/streaming/overview/request-parameters
'''


MAX_LINES = 1000
MIN_DURATION = 40
HOST = ''
# TODO: Loop through ports
PORT = 9994
FIREBASE_URL = "https://sentimentcloud.firebaseio.com/"
nlp = StanfordCoreNLP('http://localhost:8080')

# Authentication
consumer_key = 'dIberpk03sTPsn9yXiSfDoJnV'
consumer_secret = 'tpDKQSj1te1EMKb1bVKC1ofMqzr4mBhrMrqRKo4wdqXIqv03YH'
access_token = '796046817649692672-ZpmBrdDKDkjr5D9ibArLUa8rwX3sEwz'
access_token_secret = 'ssI6h7BdioCqoO2i3Hdr6PIIeQpdrU34yMvEqF5JKjiKI'
auth = requests_oauthlib.OAuth1(consumer_key,
                                consumer_secret,
                                access_token,
                                access_token_secret)


def firehose_client(conn, auth, params):
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query = url + '?' + '&'.join(k + '=' + v for k, v in params.iteritems())
    response = requests.get(query, auth=auth, stream=True)
    print(query, response)

    count = 0
    for line in response.iter_lines():
        try:
            # post = json.loads(line.decode['utf-8'])
            # post['text'], post['coordinates'], post['place'], etc.
            # tweet = post['text']
            conn.send(line + '\n')
            count += 1
            print(str(datetime.now() + ' ' + 'count: ' + str(count)))
            if count > MAX_LINES:
                break
        except:
            err = sys.exc_info()[0]
            print('Error: {}'.format(err))
            if err == socket.error:
                conn.close()
                break

    conn.close()


def socket_listener(port, params):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, port))
    except socket.error, msg:
        print('Bind failed. Error Code: ' +
              str(msg[0]) + ' Message ' + msg[1])
        print('closing...')
        sock.close()
        sys.exit(0)

    print('Socket bind complete')

    sock.listen(4)
    print('Socket listening')

    conn, addr = sock.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
    firehose_client(conn, auth, params)

    print('Closing socket...')
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    return


def create_request_params(job, lang='en'):
    keywords = job['keywords']
    if type(keywords) == list and len(keywords) > 1:
        keywords = ','.join(keywords)
    return {
        'track': keywords,
        'language': lang
    }


def run_sentiment_analyzer(tweets_str):
    res = nlp.annotate(tweets_str, properties={
                                        'annotators': 'sentiment',
                                        'outputFormat': 'json',
                                        'ssplit.eolonly': 'true'
                                    })
    sentiments = {
        'positive': 0,
        'negative': 0,
        'neutral': 0
    }

    for s in res['sentences']:
        val = int(s['sentimentValue'])
        if val == 0 or val == 1:
            sentiments['negative'] += 1
        elif val == 2:
            sentiments['neutral'] += 1
        else:
            sentiments['positive'] +=1
    
    return sentiments

def collect_tweets(jobID):
    # get newly made directories (output from spark streaming)
    job_dirs = [d for d in os.listdir('./') if d.endswith(jobID)]
    if not job_dirs:
        print('spark streaming yielded no results')
        fb.put('/jobs' + jobID, 'status', 'waiting')
        return None

    p = re.compile('(part-\d+)')
    # TODO: list comprehension doesn't work :(
    # filenames = \
    #   [jd + '/' + d for d in os.listdir(jd) if p.match(d) for jd in job_dirs]
    filenames = []
    for jd in job_dirs:
        for d in os.listdir(jd):
            if p.match(d):
                filenames.append(jd + '/' + d)

    tweets = []
    for fname in filenames:
        with open(fname) as f:
            lines = [line.strip() for line in f]
            tweets += lines

    return tweets


def start_job(jobID, job):
    params = create_request_params(job)
    try:
        duration = int(job['duration'])
        if duration < MIN_DURATION:
            duration = MIN_DURATION
    except ValueError:
        duration = MIN_DURATION

    listener_thread = Thread(target=socket_listener,
                             args=(PORT, params))
    listener_thread.deamon = True
    listener_thread.start()
    time.sleep(2)
    if not listener_thread.isAlive():
        print('Retrying bind')
        return -1

    fb.put('/jobs/' + jobID, 'status', 'in-progress')

    print('starting spark on port: {}'.format(PORT))
    spark_thread = Thread(target=spark_client.start,
                          args=(PORT, duration, jobID))
    spark_thread.start()

    spark_thread.join()
    print('spark thread finished')
    listener_thread.join()
    print('listener thread finished')

    tweets = collect_tweets(jobID)
    if not tweets:
        fb.put('/jobs/' + jobID, 'status', 'waiting')
        return -1

    print('pre-cleaning count: {}'.format(len(tweets)))
    cleaned_tweets = ['' for i in xrange(len(tweets))]
    # clean the tweets; strip unwanted text
    for i, tweet in enumerate(tweets):
        # print('tweet: {}'.format(tweet))
        # print('split: {}'.format(tweet.split('|', 1)))
        text = tweet.split('|', 1)
        if len(text) > 1:
            text = text[1].lstrip()
        else:
            text = text[0]
            text = text.split('http')[0].strip()  # remove url

        try:
            # prevent non-ascii encodable strings to coreNLP
            d = text.decode('utf-8')
            str(d)  # should cause the exception if we want to throw away
            cleaned_tweets[i] = text
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
            # print('Ill-formed tweet text')

    cleaned_tweets = [s for s in cleaned_tweets if s]
    print('post-cleaning count: {}'.format(len(cleaned_tweets)))

    # combine tweets for batch coreNLP processing
    tweets_batch = '\n'.join(cleaned_tweets)
    sentiments = run_sentiment_analyzer(tweets_batch)
    
    if sentiments:
        fb.put('/jobs/' + jobID, 'results', sentiments)

    fb.put('/jobs/' + jobID, 'status', 'completed')
    return 0

def poll_jobs(fb):
    while True:
        print('polling for jobs')
        result = fb.get('/jobs', None)
        for k, v in result.iteritems():
            if v['status'] != 'waiting':
                continue
            status_code = start_job(k, v)
            if status_code != 0:
                fb.put('/jobs/' + jobID, 'status', 'waiting')
                continue
            time.sleep(5)

        # Throttle the stream
        time.sleep(10)


if __name__ == "__main__":
    fb = firebase.FirebaseApplication(FIREBASE_URL, None)
    poll_jobs(fb)
