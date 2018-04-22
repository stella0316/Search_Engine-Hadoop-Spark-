import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
import pandas as pd
import subprocess
import json

sc = SparkContext()
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')
for f in fs.get(conf).listStatus(path):
	file_path = str(f.getPath())
	cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	out = cat.communicate()[0]
	json_string = out.decode('utf-8')
	data_chunk = json.loads(out.decode('utf-8'))
	#print(data_chunk)
	column_infos = data_chunk['meta']['view']['columns']
	column_names = [info['name'] for info in column_infos]
	#print(column_names)
	data_rows = data_chunk['data']
	# print(data_chunk['meta'])
	out = pd.DataFrame(data_rows, columns = column_names)
	print('Done with', file_path)
	out.to_csv('test.csv')

  #rdd = sc.wholeTextFiles(temp.json)
  #data = spark.read.json(rdd)
  #print(data.first())
#data = sc.read.json('hdfs:/user/bigdata/nyc_open_data/22zm-qrtq.json')
#path = sys.argv[1]
#rdd = sc.textFile(path)

# print(data.first())

