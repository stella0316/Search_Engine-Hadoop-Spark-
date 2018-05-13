module load python/gnu/3.4.4
module load spark/2.2.0

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python content_index.py
