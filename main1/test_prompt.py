from pyspark import SparkConf, SparkContext
import subprocess
import sys

name = sys.argv[1]
testFileName = name

sc = SparkContext()

fileRDD = sc.textFile(testFileName)
print(fileRDD.first())

sc.stop()
