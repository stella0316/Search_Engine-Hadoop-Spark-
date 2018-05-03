import sys
import re
import nltk
from csv import reader
from operator import add
from collections import Counter
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF, Normalizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
#from pyspark.sql import Row

porter = nltk.stem.PorterStemmer()
nltk.download('stopwords')
stopWordList = set(nltk.corpus.stopwords.words("english"))
sc = SparkContext()
spark = SparkSession.builder.appName("column index").config("spark.some.config.option", "some-value").getOrCreate()

columnName = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
masterIndex = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
rawData = columnName.join(masterIndex, columnName['Name']==masterIndex['Table_Name'])
#data.coalesce(1).write.save("column_index.csv", format='csv',header='true')
rawData = rawData.rdd

def parse(doc):
    docID = doc[0]
    docData = doc[1]
    docData = docData.lower()
    docData = re.sub(r'[^a-z0-9 ]', ' ', docData)
    docData = docData.split()
    docData = [x for x in docData if x not in stopWordList]
    docData = [porter.stem(word) for word in docData]
    return (docID, docData)

data = rawData.map(lambda x: (x['Doc_ID'], x['Columns'])).map(parse)

titles = data.map(lambda x: x[0])
documents = data.map(lambda x: x[1])
hashingTF = HashingTF()
tf = hashingTF.transform(documents)
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
normalizer = Normalizer()
tfidfData = titles.zip(normalizer.transform(tfidf)).map(lambda x: LabeledPoint(x[0], x[1]))
#MLUtils.saveAsLibSVMFile(tfidfData, "tfidf_column.out")

query = parse((0, "dBn location name location category administrative"))[1]
queryTF = hashingTF.transform(query)
queryTFIDF = normalizer.transform(idf.transform(queryTF))
queryRelevance = tfidfData.map(lambda x: (x.label, x.features.dot(queryTFIDF))).sortBy(lambda x: -x[1])
queryRelevance.saveAsTextFile("tfidf_column.out")

'''
def createPairs(doc):
    docID = doc[0]
    docData = doc[1]
    counter = Counter(docData)
    result = []
    for word, count in counter.items():
        result.append((word, [[docID, count]]))
    return result

def inSort(doc):
    sortedData = sorted(doc[1], key= lambda x: -x[1])
    result = map(lambda x: x[0], sortedData)
    return (doc[0], result)

def printResult(x):
    result = str(x[0]) + '\t' + str(x[1]) + '\t' + '('
    for location in x[2]:
        result += str(location) + ','
    result = result[0:-1]
    result += ')'
    return result

inverted_index = data.flatMap(createPairs).reduceByKey(add).map(inSort).map(lambda x: (x[0],len(x[1]),x[1])).sortBy(lambda x: -x[1]).map(printResult)
inverted_index.saveAsTextFile("column_index.out")
'''



'''
#data = sc.textFile(sys.argv[1])
#header = data.first()
#data = data.filter(lambda x: x!= header).mapPartitions(lambda x: reader(x))

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
'''
