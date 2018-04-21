import sys
import os
from pyspark import SparkContext
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *


sc = SparkContext()
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')
for f in fs.get(conf).listStatus(path):
  file_path = str(f.getPath())
  print(file_path)
  data = spark.read.json(file_path,multiLine=True)
  print(data.first())
#data = sc.read.json('hdfs:/user/bigdata/nyc_open_data/22zm-qrtq.json')
#path = sys.argv[1]
#rdd = sc.textFile(path)
print(data.first())
