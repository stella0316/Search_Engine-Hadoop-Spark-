'''
Create inverted index for searching over titles.
Extract all words from each document and match each document to the keys
'''
#spark-submit title_index.py Lower%20Manhattan%20Retailers.csv - for submitting code

import sys
from csv import reader
from pyspark import SparkContext
from operator import add


#Start PySpark
sc = SparkContext()
#sq = SQLContext(sc)

doc_id = 1 #for now
name = 'Lower Manhattan Retailers.csv' #for now just with one file name

name = name.replace('.csv','') #remove csv part

table = str.maketrans(dict.fromkeys('()!-,',' '))
name = name.translate(table)
name = name.lower() #convert to lower case

#name = name.replace('.',' ') #replace dots with spaces to then split on space
words = sc.parallelize(name.split(' ',))

inverted_index = words.map(lambda word: (word.strip(), 1)).reduceByKey(add)\
	.map(lambda pair: ((pair[0],1),(doc_id,pair[1]))) #create index: (word,frequency)(doc_id, word_frequency))

print(inverted_index.take(2)) #print to check the output looks correct

#inverted_index.saveAsTextFile("inverted_index_sample.out")



