from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.\
        master("local").\
            appName("wordCount sparksql ver").\
                getOrCreate()


df = ss.read.text("wordcount/words.txt")

# transformation
df = df.withColumn('word', f.explode(f.split(f.col('value'), " ")))\
       .withColumn("count", f.lit(1))\
       .groupby('word').sum()

df.show()