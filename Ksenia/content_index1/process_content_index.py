from pyspark.sql import SparkSession
import sys
from nltk.corpus import stopwords

stop_words = set(stopwords.words("english"))
extra_words = set(('none', '&', 'for', 'and'))

spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sc = spark.sparkContext

#read in inverted index
content_index = sc.textFile(sys.argv[1])

#print(content_index.first())

def check_integer(line, shrink=100):
	vals = line.split('\t')
	w = vals[0]
	w = w.strip().replace('.','').replace("'",'')
	try: 
		n = int(w)
		if len(w)!=4:#potentially a year
				return ('none',0,[0])
		elif n<1900 or n>2050:
				return ('none',0,[0])
		out = int(vals[1])/shrink
		return (w,out,vals[2])
	except ValueError:
		if w.isalpha():
				return (w,int(vals[1]),vals[2])
		else:
				return ('none',0,[0])

def format_output(line):
  '''
  #Format string for output
  '''
  output = line[0] + '\t' + str(line[1]) + '\t' + line[2]
  return output

content_index = content_index.map(check_integer).filter(lambda x: len(x[0])>1 and x[0] not in stop_words and x[0].strip() not in extra_words).sortBy(lambda x: x[1],ascending=False).map(format_output)
print(content_index.first())

content_index.saveAsTextFile("content_index_updated.txt")

