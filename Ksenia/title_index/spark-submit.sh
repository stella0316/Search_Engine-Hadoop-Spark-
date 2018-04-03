hfs -rm -r inverted_index_sample.out

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
title_index.py
