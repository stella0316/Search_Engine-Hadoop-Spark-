import sys
from csv import reader
from pyspark import SparkContext
from operator import add
from collections import Counter
import string
import re
#from pyspark.sql import Row
#from pyspark.sql import SparkSession

sc = SparkContext()
#spark = SparkSession.builder.appName("column index").config("spark.some.config.option", "some-value").getOrCreate()

data = sc.textFile(sys.argv[1], 1)
header = data.first()
data = data.filter(lambda x: x!= header).mapPartitions(lambda x: reader(x))

def stripwhite(text):
    lst = text.split("'")
    for i, item in enumerate(lst):
        if not i % 2:
            lst[i] = re.sub('\s+','', item)
    return "'".join(lst)

data = data.map(lambda x: (x[0], stripwhite(x[2][1:-1])))

def parse(text):
    result = []
    key_value_pairs = re.findall(r"(?:[^\s,']|'(?:\\.|[^'])*')+", text)
    for key_value_pair in key_value_pairs:
        key_value = key_value_pair.split(":")
        result.append(key_value[0][1:-1])
    return result

# (doc_id, [column_1, ..., column_n])
data = data.map(lambda x: (x[0], parse(x[1])))

def create_pairs(pair):
    '''
        Produce key value pairs:
            (word, [[doc_id, column], ..., [doc_id, column]])
    '''
    doc_id = pair[0]
    table = string.maketrans('!@#$%^&*()_+-=,./;<>?:', '                      ')
    output = []
    for column in pair[1]:
        column_norm = column.translate(table)
        column_norm = column_norm.lower()
        words = column_norm.split(' ')
        #words = set(words)
        words = filter(lambda x: x is not '', words)
        counter = Counter(words)
        for word, count in counter.items():
            output.append((word, [[doc_id, column, count]]))
        #for word in words:
            #output.append((word, [[doc_id, column]]))
    return output

def print_result(x):
    result = x[0] + '\t' + str(x[1]) + '\t' + '('
    for location in x[2]:
        result += '(' + location[0] + ',' + '\'' + location[1] + '\'' + ')' + ','
    result = result[0:-1]
    result += ')'
    return result


inverted_index = data.flatMap(create_pairs).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], len(x[1]), x[1])).sortBy(lambda x: -x[1]).map(lambda x: (x[0], x[1], sorted(x[2], key=lambda y: -y[2]))).map(print_result)#map(lambda x: Row(keyword=x[0], frequency=x[1], location_list=str(x[2])))
inverted_index.saveAsTextFile("column_index.out")
#column_index = spark.createDataFrame(inverted_index)
#column_index.show()
#column_index.coalesce(1).write.save("column_index.csv",format="csv",header="true")
