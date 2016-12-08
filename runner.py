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
# PORTS = [9991, 9992, 9993]
PORT = 9994
FIREBASE_URL = "https://sentimentcloud.firebaseio.com/"

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
                # print('Closing connection...')
                # conn.close()
                # return -1 

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


def poll_jobs(fb):
    while True:
        print('polling for jobs')
        result = fb.get('/jobs', None)
        for k, v in result.iteritems():
            if v['status'] != 'waiting':
                continue

            print k, v
            params = create_request_params(v)
            try:
                duration = int(v['duration'])
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
                break

            fb.put('/jobs/' + k, 'status', 'in-progress')

            print('starting spark on port: {}'.format(PORT))
            spark_thread = Thread(target=spark_client.start,
                                  args=(PORT, duration, k))
            spark_thread.start()

            spark_thread.join()
            print('spark thread finished')
            listener_thread.join()
            print('listener thread finished')

            # get newly made directories (output from spark streaming)
            job_dirs = [d for d in os.listdir('./') if d.endswith(k)]
            if not job_dirs:
                print('spark streaming yielded no results')
                fb.put('/jobs' + k, 'status', 'waiting')
                continue

            p = re.compile('(part-\d+)')
            # TODO: list comprehension doesn't work :(
            # filenames = [jd + '/' + d for d in os.listdir(jd) if p.match(d) for jd in job_dirs]
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
            
            print len(tweets)
            for tweet in tweets:
                print tweet


            fb.put('/jobs/' + k, 'status', 'completed')

            time.sleep(5)

        time.sleep(10)


if __name__ == "__main__":
    fb = firebase.FirebaseApplication(FIREBASE_URL, None)
    poll_jobs(fb)
