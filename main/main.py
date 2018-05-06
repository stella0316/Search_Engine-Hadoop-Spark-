import os
import sys
import re
import nltk
from pyspark.sql.functions import format_string,date_format,col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from csv import reader
from operator import add
from collections import Counter
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF, Normalizer

porter = nltk.stem.PorterStemmer()
nltk.download('stopwords')
stopWordList = set(nltk.corpus.stopwords.words("english"))
sc = SparkContext()
spark = SparkSession.builder.appName("column index").config("spark.some.config.option", "some-value").getOrCreate()

def getInput(search_type,words,row_filter):
	types = search_type.split(',')
	for t in types:
		function_dict[search_type_dict[t]](words,row_filter)

def jaccard_similarity(words,sentences):
  ps = nltk.stem.PorterStemmer()
  words = [ps.stem(word.strip().lower().replace('[','').replace(']','')) for word in words]
  words = set(words)
  out = []
  for sentence in sentences:
    list2 = re.findall(r"[\w']+", sentence)
    words2 = set([ps.stem(word.strip().lower()) for word in list2])
    score = len(set.intersection(words,words2))/len(set.union(words,words2))
    out.append(score)
  return out

def title_search(words,row_filter):
	words = words.split(',')	
	if row_filter == 'n' or row_filter == 'N':
		min_row = 0
	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()

	for w in new_list:
		w=w.replace('[','').replace(']','')
		query = "SELECT docs FROM title_search_index WHERE key like %s" % w  
		try:
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
	if len(ID_list) == 0:
		print("Sorry, nothing matched in title search, please try a different keyword")
	else:        
		re = set(ID_list[0])
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)

		query_2 = "Table_Length >= " + str(min_row) 
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			names = table.select("Table_Name").rdd.flatMap(lambda x: x).collect()                                                    
			scores = jaccard_similarity(words,names)                                                                                 
			ids = table.select("Doc_ID").rdd.flatMap(lambda x: x).collect()                                                          
			scores = spark.createDataFrame(zip(ids,scores), ('Doc_IDs','Score'))                                                     
			table = table.join(scores, table.Doc_ID == scores.Doc_IDs) 
			print("Here is your title search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).orderBy('Score',ascending=False).select(table.Table_Name,table_desc.Category, table_desc.Description,table.Score).show()
		else:
			print("Sorry, nothing matched in title search, please try a different keyword")

def parse(doc):
    docID = doc[0]
    docData = doc[1]
    docData = docData.lower()
    docData = re.sub(r'[^a-z0-9 ]', ' ', docData)
    docData = docData.split()
    docData = [x for x in docData if x not in stopWordList]
    docData = [porter.stem(word) for word in docData]
    return (docID, docData)

def column_search(words,row_filter):
    
    if row_filter == 'n' or row_filter == 'N':
        min_row = 0
    else:
        min_row = row_filter

    rawData = table_cols.join(master_index, master_index["Table_Name"]==table_cols["Name"]).rdd
    data = rawData.map(lambda x: (x['Doc_ID'], x['Columns'])).map(parse)

    titles = data.map(lambda x: x[0])
    documents = data.map(lambda x: x[1])
    hashingTF = HashingTF()
    tf = hashingTF.transform(documents)
    tf.cache()
    idf = IDF().fit(tf)
    normalizer = Normalizer()
    tfidf = normalizer.transform(idf.transform(tf))
    tfidfData = titles.zip(tfidf).toDF(["label", "features"])
    
    query = parse((0, words))[1]
    queryTF = hashingTF.transform(query)
    queryTFIDF = normalizer.transform(idf.transform(queryTF))
    queryRelevance = tfidfData.rdd.map(lambda x: (x[0], float(x[1].dot(queryTFIDF)))).sortBy(lambda x: -x[1])
    queryRelevance = queryRelevance.toDF(["Doc_ID", "scores"])
    queryRelevance = queryRelevance.join(table_desc,queryRelevance.Doc_ID == table_desc.Doc_ID).select(table_desc.Doc_ID, queryRelevance.scores, table_desc.Columns)
    queryRelevance = queryRelevance.join(master_index, master_index.Doc_ID==queryRelevance.Doc_ID).select(master_index.Table_Name, queryRelevance.Columns, queryRelevance.scores)
    queryRelevance = queryRelevance.rdd.filter(lambda x: int(x['Table_Length']) >= int(min_row)).toDF()
    queryRelevance.show()
    '''
	if row_filter == 'n' or row_filter == 'N':
		min_row = 0

	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()

	for w in new_list:
		query = "SELECT docs FROM column_search_index WHERE key like %s" % w
		try:
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)

	if len(ID_list) == 0:
		print("Sorry, nothing matched in column search, please try a different keyword")
	else: 
		re = set(ID_list[0])
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)

		query_2 = "Table_Length >= " + str(min_row)  
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			print("Here is your column search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).select(table.Table_Name,table_desc.Columns).show()
		else:
			print("Sorry, nothing matched in column search, please try a different keyword")
    '''



def content_search(words,row_filter):

	if row_filter == 'n' or row_filter == 'N':
		min_row = 0
	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()
    
	for w in new_list:
		query = "SELECT docs FROM content_search_index WHERE key like %s" % w
		try: 
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
	if len(ID_list) == 0:
		print("Sorry, nothing matched in content search, please try a different keyword")

	else: 
		re = set(ID_list[0])
    
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)
		query_2 = "Table_Length >= " + str(min_row)
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			print("Here is your content search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).select(table.Table_Name,table_desc.Category, table_desc.Description).show()
		else:
			print("Sorry, nothing matched in content search, please try a different keyword")
    


def topic_search(words,row_filter):
	if row_filter == 'n' or row_filter == 'N':
		min_row = 0
	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()
    

	for w in new_list:
		query = "SELECT Doc_ID FROM tag_index WHERE key like %s" % w 
		try: 
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)

	if len(ID_list) == 0:
		print("Sorry, nothing matched in topic search, please try a different keyword")
	else: 
		re = set(ID_list[0])
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)
	
		query_2 = "Table_Length >= " + str(min_row)
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			print("Here is your topic search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).select(table.Table_Name,table_desc.Category, table_desc.Description).show()  
		else:
			print("Sorry, nothing matched, please try a different keyword")
    


search_type_dict = {'1':'title_search','2':'column_search','3':'content_search','4':'topic_search'}
function_dict = {'title_search':title_search,'column_search':column_search,'content_search':content_search,'topic_search':topic_search}

def getInput(search_type,words,row_filter):
	types = search_type.split(',')
	for t in types:
		function_dict[search_type_dict[t]](words,row_filter)


def main():
    
	getInput(search_type,words,row_filter)


if __name__ == "__main__":
	spark = SparkSession \
        	.builder \
        	.appName("Python Spark SQL basic example") \
        	.config("spark.some.config.option", "some-value") \
        	.getOrCreate()

	sc = spark.sparkContext
	
	#get input parameters
	title_line = sc.textFile(sys.argv[1])
	column_line = sc.textFile(sys.argv[2])
	content_line = sc.textFile(sys.argv[3])
	
	#map to rdd
	title_parts = title_line.map(lambda l: l.split('\t'))
	title_rdd =  title_parts.map(lambda p: Row(key=p[0], docs = [int(p_.replace("'",'')) for p_ in p[2].replace('(','').replace(')','').split(',')]))	
	
	column_parts = column_line.map(lambda l:l.split('\t'))
	column_rdd = column_parts.map(lambda p:Row(key=p[0],
                    docs = [int(p_.replace("'",'')) for p_ in p[2].replace('(','').replace(')','').split(',')]))	
        
	content_parts = content_line.map(lambda l: l.split("\t"))
	content_rdd =  content_parts.map(lambda p: Row(key=p[0], docs = [int(p_.replace("'",'')) for p_ in p[2].replace('(','').replace(')','').split(',')]))
	
	#create table from rdds
	title_search_index = spark.createDataFrame(title_rdd)
	column_search_index = spark.createDataFrame(column_rdd)
	content_search_index = spark.createDataFrame(content_rdd)

	#create temp view for tables
	title_search_index.createOrReplaceTempView("title_search_index")
	column_search_index.createOrReplaceTempView("column_search_index")
	content_search_index.createOrReplaceTempView("content_search_index")


	#read master index from csv files
	master_index = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[4])
	master_index.createOrReplaceTempView("master_index")

	tag_index = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[5])
	tag_index.createOrReplaceTempView("tag_index")

	table_desc = spark.read.format('csv').options(header='true',inferschema='true',delimiter = ',').load(sys.argv[6])
	table_desc.createOrReplaceTempView("table_desc")

	table_cols = spark.read.format('csv').options(header='true',inferschema='true',delimiter = ',').load(sys.argv[10])

	search_type = sys.argv[7]
	words = sys.argv[8]
	row_filter = sys.argv[9]
	main()
