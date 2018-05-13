from pyspark import SparkContext
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
count=0
for f in fs.get(conf).listStatus(path):
	if count == 100:
		break
	file_path = str(f.getPath())
	cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	out = cat.communicate()[0]
	if len(out)==0:
		print('empty')
		continue
	print('Processing: ', file_path)
	data_chunk = json.loads(out.decode('utf-8'))
	column_infos = data_chunk['meta']['view']['columns']
	column_names = [info['name'] for info in column_infos]
	data_rows = data_chunk['data']
	out = pd.DataFrame(data_rows, columns = column_names)
	out.drop(['sid','id','position','created_at','created_meta','updated_at','updated_meta','meta'],axis=1,inplace=True)
	file_name = data_chunk['meta']['view']['name'] 
	file_name = file_name.replace(":","")
	#print(file_name)
	out.to_csv('nyc_processed_data/'+file_name+'.csv')
	count+=1
	print('Loaded ', str(count), 'documents')
