module load python/gnu/3.4.4
module load spark/2.2.0

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python process_json_csv.py
