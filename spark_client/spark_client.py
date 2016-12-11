'''
Parth Parikh, Nivedh Rajesh, Alessandro Orsini
Rutgers ECE494 - Cloud Computing
'''
import json
from pycorenlp import StanfordCoreNLP
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment


HOST = ''

# Currently not used for in-line sentiment analysis; see VADER implementation
nlp = StanfordCoreNLP('http://localhost:8080')
nlp_properties = {
    'annotators': 'sentiment',
    'outputFormat': 'json',
    'ssplit.eolonly': 'true'
}


def get_json(myjson):

    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return None
    return json_object


def discretized_vader(text, cutoffs=[-0.150, 0.150]):
    '''
    Give a discreted VADER sentiment score
    '''
    score = vaderSentiment(text)['compound']
    if score < cutoffs[0]:
        return 1
    elif score > cutoffs[1]:
        return 3
    return 2


def is_valid_string_format(text):
    '''
    Discard text that cannot be converted to ascii or unicode
    '''
    try:
        # prevent non-ascii encodable strings to coreNLP
        d = text.decode('utf-8')
        str(d)  # should cause the exception if we want to throw away
        return True
    except (UnicodeDecodeError, UnicodeEncodeError):
        return False


def start(port, duration=40, jobID='', batch_interval=20):
    '''
    All Spark Streaming options go here
    '''

    # Create a local StreamingContext with two working thread and
    #   batch interval of 1 second
    sc = SparkContext('local[2]', 'NetworkWordCount')
    ssc = StreamingContext(sc, batch_interval)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream(HOST, port)

    text = lines.map(lambda post: get_json(post))\
                .filter(lambda post: post is not None)\
                .filter(lambda post: 'created_at' in post)\
                .filter(lambda post: is_valid_string_format(post['text']))\
                .map(lambda post: post['created_at'] + ' | ' + post['text'])

    '''
    No write to disk option! This will compute sentiment on the fly without
    first writing all text to temporary disk storage. To utilize this map(),
    users will need to only save the sentiment counts to a file and read
    from it in the runner.py code
    '''
    # sentiment_counts = cleaned_text.map(
    #     lambda text: (discretized_vader(text), 1)
    # ).reduceByKey(lambda x, y: x + y)

    # sentiment_counts.pprint()

    text.saveAsTextFiles('./text', suffix=jobID)

    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop()
