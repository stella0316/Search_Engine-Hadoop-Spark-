import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
import pandas as pd
import subprocess
import json

'''
Process data into a desired csv form. Drop metadata
'''
sc = SparkContext()
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('hdfs:/user/bigdata/nyc_open_data/')
for f in fs.get(conf).listStatus(path):
	file_path = str(f.getPath())
	cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	out = cat.communicate()[0]
	if len(out)==0:
		print('empty')
		continue
	#json_string = out.decode('utf-8')
	print('Processing: ', file_path)
	data_chunk = json.loads(out.decode('utf-8'))
	column_infos = data_chunk['meta']['view']['columns']
	column_names = [info['name'] for info in column_infos]
	data_rows = data_chunk['data']
	out = pd.DataFrame(data_rows, columns = column_names)
	file_name = data_chunk['meta']['view']['name'] 
	out.to_csv('nyc_processed_data/'+file_name+'.csv')
	print('Done with', file_path)
