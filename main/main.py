import os
import sys
from pyspark.sql.functions import format_string,date_format,col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

def getInput(search_type,words,row_filter):
	types = search_type.split(',')
	for t in types:
		function_dict[search_type_dict[t]](words,row_filter)


def title_search(words,row_filter):

	if row_filter == 'n' or 'N':
		min_row = 0
	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()

	for w in new_list:
		query = "SELECT docs FROM title_search_index WHERE key like %s" % w  
		try:
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)
	if len(ID_list) == 0:
		print("Sorry, nothing matches, please try a different keyword")
	else:        
		re = set(ID_list[0])
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)

		query_2 = "Table_Length >= " + str(min_row) 
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			print("Here is your title search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).select(table.Table_Name,table_desc.Category, table_desc.Description).show()
		else:
			print("Sorry, nothing matches, please try a different keyword")

def column_search(words,row_filter):
	if row_filter == 'n' or 'N':
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
		print("Sorry, nothing matches, please try a different keyword")
	else: 
		re = set(ID_list[0])
		for s in ID_list[1:]:
			re.intersection_update(s)
		re = list(re)

		query_2 = "Table_Length >= " + str(min_row)  
		table = master_index.where(col("Doc_ID").isin(re)).filter(query_2)
		if table.count() != 0:
			print("Here is your column search result")
			result = table.join(table_desc,table.Doc_ID == table_desc.Doc_ID).select(table.Table_Name,table_desc.Category, table_desc.Description).show()
		else:
			print("Sorry, nothing matches, please try a different keyword") 


def content_search(words,row_filter):

	if row_filter == 'n' or 'N':
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
		print("Sorry, nothing matches, please try a different keyword")

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
			print("Sorry, nothing matches, please try a different keyword")
    


def topic_search(words,row_filter):
	if row_filter == 'n' or 'N':
		min_row = 0
	else:
		min_row = row_filter
  
	new_list = ['"%' + w.strip().lower() + '%"' for w in words]
	ID_list = list()
    

	for w in new_list:
		query = "SELECT docs FROM tag_index WHERE key like %s" % w 
		try: 
			IDs = spark.sql(query).collect()[0][0]
		except IndexError:
			continue
		ID_list.append(IDs)

	if len(ID_list) == 0:
		print("Sorry, nothing matches, please try a different keyword")
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
			print("Sorry, nothing matches, please try a different keyword")
    


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

	table_desc = spark.read.format('csv').options(header='true',inferschema='true',delimiter = ',').load(sys.argv[6])
	table_desc.createOrReplaceTempView("table_desc")
	
	search_type = sys.argv[7]
	words = sys.argv[8]
	row_filter = sys.argv[9]
	main()
