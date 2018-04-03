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

name = 'Lower Manhattan Retailers.csv' #for now just with one file name

words = sc.parallelize(name.split(' '))

inverted_index = words.map(lambda word: (word, 1)).reduceByKey(add)\
	.map(lambda pair: (pair[0],(name,pair[1])))

print(inverted_index.take(2))

inverted_index.saveAsTextFile("inverted_index_sample.out")



