import sys
import os
from pyspark import SparkContext
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *


sc = SparkContext()
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

path = sys.argv[1]
rdd = sc.textFile(path)
print(rdd.first())