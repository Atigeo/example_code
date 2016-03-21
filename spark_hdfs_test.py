#!/usr/bin/python
import sys
from pyspark import SparkConf, SparkContext

if len(sys.argv) != 2:
  print "Please pass in the spark master url: ex $ spark_hdfs_test.py spark_master"
  exit()

spark_master = str(sys.argv[1])

conf = (SparkConf()
         .setMaster("spark://"+spark_master+":7077")
         .setAppName("TestApp")
         .set("spark.executor.memory", "1g")
         .set("spark.executor.cores", "1" )) 
sc = SparkContext(conf = conf)

try:
  sc.parallelize(['Its fun to have fun,','but you have to know how.']).saveAsTextFile("hdfs://"+spark_master+":8020/test/test.txt") 
except:
  print "File must already exist"
  
lines = sc.textFile("hdfs://"+spark_master+":8020/test/test.txt" )

wordcounts = lines.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)

print wordcounts.take(10)

