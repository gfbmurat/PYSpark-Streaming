from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from collections import namedtuple

# create spark configuration
conf = SparkConf()
conf.setMaster("local[4]").setAppName("TwitterStreamApp").setExecutorEnv("spark.executor.memory","4g").setExecutorEnv("spark.driver.memory","4g")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# creat the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 5)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",60127)

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        sql_context=get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(location=w[0], location_count=w[1]))
        # create a DF from the Row RDD
        location_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        location_df.registerTempTable("locs")
        # get the top 10 hashtags from the table using SQL and print them
        loc_counts_df = sql_context.sql("select location, location_count from locs order by location_count desc limit 10")
        loc_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        #send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


#withHashtag=dataStream.filter(lambda w: "#deprem" in w)

place_counts=dataStream.map(lambda x:(x,1))
placeTotal=place_counts.updateStateByKey(aggregate_tags_count)
placeTotal.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
