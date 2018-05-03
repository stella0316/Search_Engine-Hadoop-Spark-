import os
import sys
from pyspark.sql.functions import format_string,date_format,col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row


def prompt():
	while True:
		search_type = input("Select type(s) of search you want to do:\n 1 = Title, 2= Column, 3= Content, 4 = Topic\n Separate by comma if you select multiple types:\n" )


		values = search_type.split(',')
		wrong=False

		for i in values:
			if i not in ['1','2','3']:
				print('Input can only be integers: 1,2,3 separated by comma')
				wrong = True
			if wrong == True:
				continue
                
		if wrong == False:
			break
    
           
	keywords = input("Enter keywords for your search separated by comma:\n")

	while len(keywords) == 0 or keywords == ' ':
		print('Please enter valid input')
		keywords = input("Enter keywords for your search separated by comma")

	words = keywords.split(',')

    	#Filter
	row_filter = input("Please enter minimum number of rows per table. Enter n to ignore:\n")

	while len(row_filter) != 1:
		print('Please enter valid input')
		row_filter = input("Please enter minimum number of rows per table. Enter n to ignore:\n")

	if row_filter == 'n' or 'N':
		pass
	else:
		while True:
			try:
				row_filter = int(row_filter)
				break
			except ValueError:
				print('Please enter valid input')
				row_filter = input("Please enter minimum number of rows per table. Enter n to ignore")
                
	return search_type, words, row_filter

def getInput(search_type,words,row_filter):
	types = search_type.split(',')
	for t in types:
		function_dict[search_type_dict[t]](words,row_filter)

def title_search(words,row_filter):

	if row_filter == 'n' or 'N':
		min_row = 0
	else:
		min_row = int(row_filter)
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()

	for w in new_list:
		query = "SELECT docs FROM title_search_index WHERE key like %s" % w  
		try:
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
        
	re = set(ID_list[0])
	for s in ID_list[1:]:
		re.intersection_update(s)
	re = list(re)

	query_2 = "Table_Length >= " + row_filter  
	result = master_index.where(col("Doc_ID").isin(re)).filter(query_2).select(master_index.Table_Name)
    
	return result.show()

def column_search(words,row_filter):
	if row_filter == 'n' or 'N':
		min_row = 0

	else:
		min_row = int(row_filter)
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()

	for w in new_list:
		query = "SELECT docs FROM column_search_index WHERE key like %s" % w
		try:
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
        
	re = set(ID_list[0])
	for s in ID_list[1:]:
		re.intersection_update(s)
	re = list(re)


	query_2 = "Table_Length >= " + row_filter  
	result = master_index.where(col("Doc_ID").isin(re)).filter(query_2).select(master_index.Table_Name)
    
	return result.show()


def content_search(words,row_filter):

	if row_filter == 'n' or 'N':
		min_row = 0
	else:
		min_row = int(row_filter)
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()
    
	for w in new_list:
		query = "SELECT docs FROM content_search_index WHERE key like %s" % w
		try: 
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
        
	re = set(ID_list[0])
    
	for s in ID_list[1:]:
		re.intersection_update(s)
	re = list(re)
	
	query_2 = "Table_Length >= " + row_filter  
	result = master_index.where(col("Doc_ID").isin(re)).filter(query_2).select(master_index.Table_Name)
    
	return result.show()


def topic_search(words,row_filter):
	if row_filter == 'n' or 'N':
		min_row = 0
	else:
		min_row = int(row_filter)
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()
    

	for w in new_list:
		query = "SELECT docs FROM tag_index WHERE key like %s" % w 
		try: 
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
        
	re = set(ID_list[0])
	for s in ID_list[1:]:
		re.intersection_update(s)
	re = list(re)
	
	query_2 = "Table_Length >= " + row_filter  
	result = master_index.where(col("Doc_ID").isin(re)).filter(query_2).select(master_index.Table_Name)
    
	return result.show()


search_type_dict = {'1':'title_search','2':'column_search','3':'content_search','4':'topic_search'}
function_dict = {'title_search':title_search,'column_search':column_search,'content_search':content_search,'topic_search':topic_search}

def getInput(search_type,words,row_filter):
	types = search_type.split(',')
	for t in types:
		function_dict[search_type_dict[t]](words,row_filter)


def main():
	#search_type, words, row_filter = ('3', ['new','york', 'taxi'], '3')
    
	getInput(search_type,words,row_filter)
    
	#print('Your results', '\n', 'titles: ', title_result, '\n', 'columns:')


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
                    docs = [int(_l) for _l in p[2].replace('(','').replace(')','').split(',')[0::2]],
                    cols = [p[2].replace('(','').replace(')','').split(',')[1::2]]))	
        
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

	table_desc = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[6])
	table_desc.createOrReplaceTempView("table_desc")
	
	search_type = sys.argv[7]
	words = sys.argv[8]
	row_filter = sys.argv[8]
	main()
