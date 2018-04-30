'''
Create inverted index for searching over contents.
Extract all words from each document and match each document to the keys
'''
#spark-submit title_index.py Lower%20Manhattan%20Retailers.csv - for submitting code

import sys
from csv import reader
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
from collections import Counter
import pandas as pd
from itertools import chain

sc = SparkContext()

spark = SparkSession \
  .builder \
  .appName("Session") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()

#read metadata table and map to (file_name, id)
id_table = sc.textFile('Master_Index_New.csv').mapPartitions(lambda line: reader(line, delimiter=',',quotechar='"')).filter(lambda line: line[0]!='Doc_ID').map(lambda x: (x[2],x[0]))
id_vals = id_table.collect() #convert to a list

temped = False

for pair in id_vals:
	file_name = pair[0]
	file_id = pair[1]
	data = spark.read.format('csv').options(header=True, inferSchema=True).load('hdfs://dumbo/user/ks4841/nyc_processed_data/' + file_name + '.csv')
	#if data.count()>1000000: #if more than a million rows
	#	continue
	data = data.rdd.map(tuple).map(lambda line: (file_id, line)) #add file id to each line of file
	#print(data.first())
	if temped:
		joined = joined.union(data)
	else:
		joined = data.cache()
		temped = True
	
#print(joined.take(2))


def create_pairs(pair):
  '''
  Produce key value pairs: (word,frequency)(doc_id, word_frequency))
  '''
  doc_id = pair[0]
  text = pair[1]
  df = pd.Series(text)
  df = df.apply(str)
  #df = df[df.apply(lambda x: isinstance(x,str))]#remove all non-strings
  table = str.maketrans(dict.fromkeys('\n()\*,"!-',' '))
  df = df.str.translate(table) #remove unnecessary elements
  df = df.str.lower() #convert to lower case
  df = df.str.split()
  word_list = df.tolist()
  words = list(chain.from_iterable(word_list))
  #words = map(lambda x: x.strip(),words)#remove trailing whitespaces                                                                      
  #words = filter(None, words)#remove Nans                                                                                                 
  counter = Counter(words)     

  output = [] 
  for word, count in counter.items():
    output.append((word,count))                                                                                                         
  output = map(lambda x: ((x[0], doc_id), x[1]),output)                                                                                 
  return output  

def format_output(line):
  '''
  #Format string for output
  '''
  doc_list = line[2]
  docs = []
  for doc in doc_list:
    docs.append(doc[0])
  docs = ','.join(docs)
  output = line[0] + '\t' + str(line[1]) + '\t' + '(' + docs + ')'
  return output

#Preprocess text data: remove title, spaces, unnecessary symbols.
inverted_index = joined.flatMap(create_pairs).reduceByKey(lambda a,b: a+b)
inverted_index = inverted_index.map(lambda line: (line[0][0],((line[0][1],line[1]),))).reduceByKey(lambda a,b: a+b)
#print(inverted_index.take(2))
inverted_index = inverted_index.map(lambda line: (line[0], len(line[1]), line[1])).sortBy(lambda x: x[1],ascending=False)\
.map(lambda x: (x[0], x[1], sorted(x[2], reverse=True))).map(format_output) #organize and sort
print(inverted_index.take(2)) #print to check the output looks correct
inverted_index.saveAsTextFile("content_index.txt")
