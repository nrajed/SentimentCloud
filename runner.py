'''
Parth Parikh, Nivedh Rajesh, Alessandro Orsini
Rutgers ECE494 - Cloud Computing
'''
import argparse
import json
import math
import oauth2 as oauth
import os
import re
import requests
import requests_oauthlib
import shutil
import socket
import sys
import thread
import time
import timeit
from datetime import datetime
from firebase import firebase
from pycorenlp import StanfordCoreNLP
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from spark_client import spark_client
from threading import Thread
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment


'''
Credit for template:
    Analyze Social Media Data in Real Time -
        https://bigdatauniversity.com/blog/analyze-social-media-data-real-time
    Github -
        https://github.com/saeedaghabozorgi/TwitterSparkStreamClustering

Twitter Streaming API Request Parameters:
    https://dev.twitter.com/streaming/overview/request-parameters
'''


MAX_LINES = 100000  # Cap the number of lines processed per BATCH_INTERVAL
MIN_DURATION = 40  # Enforce a minimum streaming job duration
BATCH_INTERVAL = 20  # if split processing is enabled, default BATCH_INTERVAL
HOST = ''  # All ports will belong to localhost

# FIREBASE_URL = "https://sentimentcloud.firebaseio.com/"
nlp = StanfordCoreNLP('http://localhost:8080')

# TODO: This will be removed shortly after the course ends and will need
#   a configuration file with Twitter Authentication
consumer_key = 'dIberpk03sTPsn9yXiSfDoJnV'
consumer_secret = 'tpDKQSj1te1EMKb1bVKC1ofMqzr4mBhrMrqRKo4wdqXIqv03YH'
access_token = '796046817649692672-ZpmBrdDKDkjr5D9ibArLUa8rwX3sEwz'
access_token_secret = 'ssI6h7BdioCqoO2i3Hdr6PIIeQpdrU34yMvEqF5JKjiKI'
auth = requests_oauthlib.OAuth1(consumer_key,
                                consumer_secret,
                                access_token,
                                access_token_secret)


def firehose_client(conn, auth, params):
    '''
    Set up Twitter Streaming API parameters and
    begin writing to socket
    '''

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
            # print('Error: {}'.format(err))
            if err == socket.error:
                conn.close()
                break

    conn.close()


def socket_listener(port, params):
    '''
    Bind a listener to socket at specified port.
    If the port is busy, terminate the thread and the close the connection.
    '''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, port))
    except socket.error, msg:
        print('Bind failed. Error Code: ' +
              str(msg[0]) + ' Message: ' + msg[1])
        print('closing...')
        sock.close()
        sys.exit(0)

    print('Socket bind complete')

    sock.listen(4)
    print('Socket listening')

    conn, addr = sock.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
    firehose_client(conn, auth, params)

    # gracefully shutdown and close socket when Twitter Streaming finishes
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


def run_vader_sentiment_analyzer(batch, sentiments, cutoffs=[-0.50, 0.50]):
    '''
    One of the NLP tools used to evaluate sentiment.
    VADER outputs a compound score between -1 and +1 so we must make
    our own wrapper to categorize the scores the way we see fit.
    Cutoffs for negative, nuetral, and positive can be optionally specified.
    '''
    for text in batch:
        vs = vaderSentiment(text)
        score = vs['compound']
        if score < cutoffs[0]:
            sentiments['negative'] += 1
        elif score > cutoffs[1]:
            sentiments['positive'] += 1
        else:
            sentiments['neutral'] += 1


def run_corenlp_sentiment_analyzer(text_batch, sentiments):
    '''
    One of the NLP tools used to evaluate sentiment.
    CoreNLP outputs a score in [0, 5] that corresponds from Very Negative (0)
    to Very Positive (5), but these two extremes are extremely rare so we
    decide to lump them into three general categories.
    In the nlp.annotate(properties=) flag, we can send in large strings
    delimited by newline characters to be processed all at once. To avoid
    computation timeout, the calling function should appropriately avoid
    sending too-large strings.
    '''
    res = nlp.annotate(text_batch, properties={
                                        'annotators': 'sentiment',
                                        'outputFormat': 'json',
                                        'ssplit.eolonly': 'true'
                                    })

    for s in res['sentences']:
        val = int(s['sentimentValue'])
        if val in [0, 1]:
            sentiments['negative'] += 1
        elif val == 2:
            sentiments['neutral'] += 1
        else:
            sentiments['positive'] += 1


def collect_text(jobID):
    '''
    If NLP computation is not done on the fly (this is implemented, but
    not enabled), we must collect all the spark streaming job's relevant text
    output. 
    '''

    # get newly made directories (output from spark streaming)
    job_dirs = [d for d in os.listdir('./') if d.endswith(jobID)]
    if not job_dirs:
        print('spark streaming yielded no results')
        fb.put('/jobs/' + jobID, 'status', 'waiting')
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

    text = []
    for fname in filenames:
        with open(fname) as f:
            lines = [line.strip() for line in f]
            text += lines

    for job_dir in job_dirs:
        shutil.rmtree(job_dir)

    return text


def clean_text(input_text):
    '''
    No longer needed. This function should be called and modified to
    clean the text from the input source. Most NLP tools cannot processed
    non-unicode or non-ascii input, e.g. emojis.
    '''
    cleaned_text = ['' for i in xrange(len(input_text))]
    # clean the input text; strip unwanted text
    for i, tweet in enumerate(input_text):
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
            cleaned_text[i] = text
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
            # print('Ill-formed tweet text')

    cleaned_text = [s for s in cleaned_text if s]


def start_job(jobID, job, config):
    '''
    This function handles most of the logic.
    It essentially is responsible for spawning new threads,
    waiting on them to join, and publishing to Firebase.
    '''

    port = config['port']
    split_jobs = config['split_jobs']
    sentiment_analyzer = config['sentiment_analyzer']

    # Create request parameters and ensure minimum duration
    params = create_request_params(job)
    try:
        duration = int(job['duration'])
        if duration < MIN_DURATION:
            duration = MIN_DURATION
    except ValueError:
        duration = MIN_DURATION
    duration = [duration]

    # Handle case where user wants to do split processing
    num_jobs = 1
    if split_jobs:
        num_jobs = duration[0] // BATCH_INTERVAL
        remainder = duration[0] % BATCH_INTERVAL
        duration = [BATCH_INTERVAL for i in xrange(num_jobs)]
        if remainder != 0:
            num_jobs += 1
            duration.append(remainder)

    sentiments = {
        'positive': 0,
        'negative': 0,
        'neutral': 0
    }

    for j in xrange(num_jobs):

        print('Executing batch {} of {}'.format(j+1, num_jobs))

        # Spawn a listener thread, sleep for 2 seconds to ensure connectivity
        listener_thread = Thread(target=socket_listener,
                                 args=(port, params))
        listener_thread.deamon = True
        listener_thread.start()
        time.sleep(2)
        # Check if binding was successful
        if not listener_thread.isAlive():
            print('Retrying bind')
            return -1

        # Upon successful binding, set job status as in-progress and begin
        fb.put('/jobs/' + jobID, 'status', 'in-progress')

        print('starting spark on port: {}'.format(port))

        # Begin Spark Streaming thread
        spark_thread = Thread(target=spark_client.start,
                              args=(port, duration[j], jobID, BATCH_INTERVAL))
        spark_thread.start()

        # Wait for both threads to join again
        spark_thread.join()
        print('spark thread finished')
        listener_thread.join()
        print('listener thread finished')

        # collect text from files if they have been written to files
        # as it stands currently, all text is temporarily written to files, but
        # this can be removed completely (see spark_client.py code)
        text = collect_text(jobID)
        if not text:
            continue
        
        # text = clean_text(text)

        # TODO: This can be parallelized
        # Sentiment Analyzer batches to avoid timeout/computation overload
        sa_batch_size = 500
        N = len(text)
        sa_batch = [text[i:i+sa_batch_size]
                    for i in xrange(0, N, sa_batch_size)]
        for batch in sa_batch:
            if sentiment_analyzer == 'corenlp':
                run_corenlp_sentiment_analyzer('\n'.join(batch), sentiments)
            elif sentiment_analyzer == 'vader':
                run_vader_sentiment_analyzer(batch, sentiments)
            else:
                pass
            fb.put('/jobs/' + jobID, 'results', sentiments)

    print('=======FINISHING=======')
    fb.put('/jobs/' + jobID, 'status', 'completed')
    return 0


def poll_jobs(fb, config):
    '''
    Continuously poll for new jobs from Firebase
    '''

    while True:
        print('polling for jobs')
        result = fb.get('/jobs', None)
        for k, v in result.iteritems():
            if v['status'] != 'waiting':
                continue
            status_code = start_job(k, v, config)
            # disconnection or job failure
            if status_code != 0:
                fb.put('/jobs/' + k, 'status', 'waiting')
                continue
            # throttle job processing in between multiple jobs
            # ensures ports/sockets are ready 
            time.sleep(5)

        # Throttle the job polling
        time.sleep(10)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    msg = 'Firebase URL. Required! Currently only supports public db. ' +\
          'Ex: https://sentimentcloud.firebaseio.com'
    parser.add_argument('--firebase', help=msg)

    msg = '[Y/N].' + 'Split duration of all jobs into ' +\
          'BATCH_INTERVAL/Job_Duration segments. ' +\
          'N by default.'
    parser.add_argument('--split-jobs', help=msg)

    msg = 'Choice of sentiment analyzer. ' +\
          'Currently supported: Stanford [coreNLP], GATech [VADER]. ' +\
          'Note: [coreNLP], if used, must be listening on port  8080' +\
          'Default is VADER'
    parser.add_argument('--sa', help=msg)

    msg = 'localhost port number. Default is 9992'
    parser.add_argument('--port', help=msg)

    msg = '[Seconds] to wait for new jobs before timing out. Default is 6000'
    parser.add_argument('--timeout', help=msg)

    args = parser.parse_args()

    config = {
        'split_jobs': False,
        'sentiment_analyzer': 'vader',
        'port': 9992,
        'timeout': 6000
    }

    if args.firebase:
        firebase_url = args.firebase.lower()
    else:
        raise ValueError('Must provide Firebase URL!')
    if args.split_jobs:
        if args.split_jobs.lower() == 'y':
            config['split_jobs'] = True
    if args.sa:
        config['sentiment_analyzer'] = args.sa.lower()
    if args.port:
        try:
            config['port'] = int(args.port)
        except ValueError:
            pass
    if args.timeout:
        try:
            config['timeout'] = int(args.timeout)
        except ValueError:
            pass

    fb = firebase.FirebaseApplication(firebase_url, None)
    poll_jobs(fb, config)
