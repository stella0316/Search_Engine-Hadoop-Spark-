import sys
import os
from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
import pandas as pd
import subprocess
import json

'''
Process data into a desired csv form. Drop metadata
'''
spark = SparkSession \
	.builder \
	.appName("Python Spark SQL basic example") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

#sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')
count=0
for f in fs.get(conf).listStatus(path):
	#if count == 100:
		#break
	file_path = str(f.getPath())
	#cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	#out = cat.communicate()[0]
	#if len(out)==0:
		#print('empty')
		#continue
	#json_string = out.decode('utf-8')
	print('Processing: ', file_path)
	df = spark.read.json(file_path, multiLine=True)
	rddd = df.select('data').rdd
	mapped = rddd.flatMap(lambda x: x[0])
	out = mapped.map(lambda x: x[8:-1])
	name = df.select('meta.view.name').collect()[0][0]
	out.saveAsTextFile('processed_data/' + name)
	count+=1
	print('Processed: ', str(count))
