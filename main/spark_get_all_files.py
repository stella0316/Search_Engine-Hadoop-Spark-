
import sys
import os
from pyspark import SparkContext
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
import subprocess
import json
import numpy as np


spark = SparkSession \
	.builder \
	.appName("Python Spark SQL basic example") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

sc = spark.sparkContext


hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')

table_name_list = list()
table_category_list = list()
table_descr_list = list()
table_size_list = list()
table_columns_list = list()

num = 1

for f in fs.get(conf).listStatus(path):
	file_path = str(f.getPath())
	cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	out = cat.communicate()[0]
	if len(out) != 0:
		df = spark.read.option("multiline", "true").json(file_path)

		name = df.select("meta.view.name").collect()[0][0]
		table_name_list.append(name)
		
		try:
			descr = df.select("meta.view.description").collect()
		except:
			descr = "Missing"
		table_descr_list.append(descr)

		try:
			category = df.select("meta.view.category").collect()
		except:
			category = "Missing"

		table_category_list.append(category)

		#data = df.select("data").collect()
		table_size_list.append(len(out))

		schema_list = df.select(explode("meta.view.columns").alias("col")).select("col.name").collect()
		table_columns_list.append(schema_list)
		#schema_l = list()
		#for i in schema_list:
		#	schema_l.append(sc.parallelize(i).take(1)[0])

		#fields = [StructField(field_name, StringType(), True) for field_name in schema_l]
		#schema = StructType(fields)
	
		#raw_data = spark.createDataFrame(data[0][0],schema)
		#raw_data.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("raw1.csv")
		print("%s file done" % num)
		num += 1
	else:
		continue
table_id = np.arange(len(table_name_list))
t = pd.DataFrame({'ID':table_id,'Name': table_name_list,'Description':table_descr_list,'Category':table_category_list,'Size':table_size_list})

	
sc.stop()
