
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

sc = SparkContext()
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')

table_name_list = list()
table_category_list = list()
table_descr_list = list()

for f in fs.get(conf).listStatus(path):
	file_path = str(f.getPath())
	df = spark.read.option("multiline", "true").json(file_path)

	name = df.select("meta.view.name").collect()
	table_id = df.select("meta.view.id").collect()
	descr = df.select("meta.view.description").collect()
	category = df.select("meta.view.category").collect()

	data = df.select("data").collect()
	table_size = len(data)

	t = pd.DataFrame({'ID':table_id,'Name': name,'Description':descr,'Category':category,'Size':table_size})
	schema_list = df.select(explode("meta.view.columns").alias("col")).select("col.name").collect()

	schema_l = list()
	for i in schema_list:
        schema_l.append(sc.parallelize(i).take(1)[0])

	fields = [StructField(field_name, StringType(), True) for field_name in schema_l]
	schema = StructType(fields)
	raw_data = spark.createDataFrame(data[0][0],schema)
	raw_data.show()