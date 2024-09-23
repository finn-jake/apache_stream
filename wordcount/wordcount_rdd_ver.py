import os

# Set spark environments
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.\
        master("local").\
            appName("wordCount RDD ver").\
                getOrCreate()

    sc: SparkContext = ss.sparkContext

    # load data
    text_file: RDD[str] = sc.textFile("wordcount/words.txt")

    # transformation
    counts = text_file.flatMap(lambda line : line.split(' '))\
        .map(lambda word: (word, 1))\
            .reduceByKey(lambda count1, count2: count1 + count2)

    # action
    output = counts.collect() 

    for (word, count) in output:
        print(f"{word} : {count}")