'''
Create inverted index for searching over titles.
Extract all words from each document and match each document to the keys
'''
#spark-submit title_index.py Lower%20Manhattan%20Retailers.csv - for submitting code

import sys
from csv import reader
from pyspark import SparkContext
from operator import add
from collections import Counter

#Start PySpark
sc = SparkContext()

#Read in table with file names and ID's
data = sc.textFile('Table_Describe.csv')\
	.mapPartitions(lambda line: reader(line, delimiter=',',quotechar='"')).filter(lambda line: line[0]!='Doc_ID')
#data = data.map(lambda line: line.split(","))
#header = data.first()
#data = data.filer(lambda line: line != header)

data = data.map(lambda line: (line[0], line[1])) #select Doc ID and file name
#print(data.take(2))




def create_pairs(pair):
	'''
	Produce key value pairs: (word,frequency)(doc_id, word_frequency))
	'''
	doc_id = pair[0] #for now
	#name = 'Lower Manhattan Retailers.csv' #for now just with one file name
	name = pair[1] 
	name = name.replace('.csv','') #remove csv part
	table = str.maketrans(dict.fromkeys('()!-,',' '))
	name = name.translate(table)
	name = name.lower() #convert to lower case

	words = name.split(' ')
	words = map(lambda x: x.strip(),words)
	words = filter(None, words)
	counter = Counter(words)

	output = []
	for word, count in counter.items():
		output.append((word,count))

	output = map(lambda x: (x[0],((doc_id, x[1]),)),output)
	return output

#ind = data.map(lambda pair: (pair[0],pair[1]))
#print(ind.take(2))

inverted_index = data.flatMap(create_pairs).reduceByKey(lambda a,b: a+b)

#inverted_index = data.map(lambda pair: (pair[0], pair[1]))

inverted_index = inverted_index.map(lambda line: (line[0], len(line[1]), line[1]))

print(inverted_index.take(2)) #print to check the output looks correct

inverted_index.saveAsTextFile("inverted_index_sample.out")



