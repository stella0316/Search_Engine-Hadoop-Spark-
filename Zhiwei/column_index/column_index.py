import sys
from csv import reader
from pyspark import SparkContext
from operator import add
from collections import Counter
import string
import re

sc = SparkContext()

data = sc.textFile('Table_Describe.csv', 1)
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
        Produce key value pairs: (word, (doc_id, column, frequency))
        '''
    doc_id = pair[0]
    table = string.maketrans('()!-,', '     ')
    output = []
    for column in pair[1]:
        column = column.translate(table)
        column = column.lower()
        words = column.split(' ')
        words = map(lambda x: x.strip(), words)
        words = filter(None, words)
        counter = Counter(words)
        for word, count in counter.items():
            output.append((word, (doc_id, column, count)))
    return output

inverted_index = data.flatMap(create_pairs).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], len(x[1])/3, x[1]))
inverted_index.saveAsTextFile("inverted_index_on_column_name_sample.out")
