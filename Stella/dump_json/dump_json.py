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

path = sys.argv[1]
df = spark.read.option("multiline", "true").json(path)

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
#print(len(data[0][0][0]))
raw_data.show()
