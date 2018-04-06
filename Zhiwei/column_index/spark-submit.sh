hfs -rm -r inverted_index_on_column_name_sample.out

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
column_index.py

