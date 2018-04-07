
module load python/gnu/3.4.4
module load spark/2.2.0

FILENAME="inverted_index_sample.out"

/usr/bin/hadoop fs -rm -r "$FILENAME"

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
title_index.py

rm "$FILENAME"

/usr/bin/hadoop fs -getmerge "$FILENAME" "$FILENAME"




