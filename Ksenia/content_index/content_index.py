'''
Create inverted index for searching over contents.
Extract all words from each document and match each document to the keys
'''
#spark-submit title_index.py Lower%20Manhattan%20Retailers.csv - for submitting code

import sys
from csv import reader
from pyspark import SparkContext
from operator import add
from collections import Counter

sc = SparkContext()

#read metadata table and map to (file_name, id)
id_table = sc.textFile('Table_Describe.csv').mapPartitions(lambda line: reader(line, delimiter=',',quotechar='"')).filter(lambda line: line[0]!='Doc_ID').map(lambda x: (x[1],x[0]))

#read files from folder 
data = sc.wholeTextFiles('data/*')
path = 'hdfs://dumbo/user/ks4841/data/'
len_path = len(path)
len_csv = len('.csv')
data = data.map(lambda x: (x[0][len_path:-len_csv],x[1]))#extract file name from path

#Match file names with ID-> (ID,Text)
joined = data.leftOuterJoin(id_table).map(lambda y: (y[1][1],y[1][0]))


def create_pairs(pair):
  '''
  Produce key value pairs: (word,frequency)(doc_id, word_frequency))
  '''
  doc_id = pair[0]
  name = pair[1]
  table = str.maketrans(dict.fromkeys('\n()\"!-,',' '))
  name = name.translate(table) #remove unnecessary elements
  name = name.lower() #convert to lower case
  name.replace
  words = name.split(' ')                                                                                                                 
  words = map(lambda x: x.strip(),words)#remove trailing whitespaces                                                                      
  words = filter(None, words)#remove Nans                                                                                                 
  counter = Counter(words)                                                                                                                                                                                                             
  output = []                                                                                                                             
  for word, count in counter.items():                                                                                                     
    output.append((word,count))                                                                                                                                                                                
  output = map(lambda x: (x[0],((doc_id, x[1]),)),output)                                                                                 
  return output  

def format_output(line):
	'''
	Format string for output
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
inverted_index = inverted_index.map(lambda line: (line[0], len(line[1]), line[1])).sortBy(lambda x: x[1],ascending=False)\
	.map(lambda x: (x[0], x[1], sorted(x[2], key = lambda y: -y[1]))).map(format_output) #organize and sort
print(inverted_index.take(2)) #print to check the output looks correct

inverted_index.saveAsTextFile("content_index.txt")   


