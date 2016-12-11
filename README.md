# SentimentCloud

Parth Parikh, Nivedh Rajesh, Alessandro Orsini
Rutgers - ECE494

## Setup Requirements:

1. **Spark Streaming, CoreNLP, VADER, python-firebase**

  Apache Spark with Streaming:
  ```
  http://spark.apache.org/streaming/
  ```
  Do not forget to set your environment variables!

  We use Stanford's CoreNLP 3.7.0 beta found at:
  ```
  http://stanfordnlp.github.io/CoreNLP/
  ```
  This is relatively large download so if you wish to continue only with VADER, disable the code relevant to CoreNLP.

  Georgia Tech's VADER:
  ```
  https://github.com/cjhutto/vaderSentiment
  ```
  We recommend VADER for social media text analysis. Check out their repo.

  python-firebase module:
  ```
  https://pypi.python.org/pypi/python-firebase/1.2
  ```

  All other dependencies can be easily installed using pip.
   

2. **Environment Variables**


  You must have the SPARK_HOME and PYTHON_PATH environment variables set up. An **example** configuration is as follows:
  
  ```
  export SPARK_HOME=/usr/local/spark
  export PATH=$PATH:$SPARK_HOME/bin
  export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
  ```

  Of course, this is system and implementation dependent, but essentially all that is required is that the Python interpretor knows where it can find pyspark. The sample above has been provided under ```env_vars.sh``` To run, you must first modify permissions:
  ```
  chmod +x env_vars.sh
  ```

3. **Firebase setup**

  Users must have provide their own Firebase URL as a command-line argument. An example (which can be used until our Firebase goes private):

  ```
  python runner.py --firebase https://sentimentcloud.firebaseio.com
  ```

  This will start the program - which usually sits on EC2, but can be run from your own machine - which will poll Firebase for new jobs. To test that it works, you may run the ``` example_job.py ``` file to push a job.
  
## Running

To see available command line arguments:
```
python runner.py -h
```

**Example Run**:
```
python example_job.py
python runner.py --firebase https://sentimentcloud.firebaseio.com
```
