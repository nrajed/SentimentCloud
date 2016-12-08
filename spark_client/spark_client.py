import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


HOST = ''


def get_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return None
    return json_object


def start(port, duration=40, jobID=''):
    BATCH_INTERVAL = 20  # How frequently to update (seconds)

    # Create a local StreamingContext with two working thread and
    #   batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, BATCH_INTERVAL)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream(HOST, port)

    tweets = lines.map(lambda post: get_json(post))\
                  .filter(lambda post: post is not None)\
                  .filter(lambda post: 'created_at' in post)\
                  .map(lambda post: post['created_at'] + post['text'])

    tweets.pprint()
    tweets.saveAsTextFiles('./tweets', suffix=jobID)
    # Split each line into words
    # words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    # pairs = words.map(lambda word: (word, 1))
    # wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this
    #   DStream to the console
    # wordCounts.pprint()

    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop()
