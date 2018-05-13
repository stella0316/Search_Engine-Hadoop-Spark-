
module load python/gnu/3.4.4
module load spark/2.2.0

SEARCH_TYPES="$1"
WORDS="$2"
FILTER="$3"

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
main.py data/title_index.txt data/column_index.txt data/content_index.txt data/Master_Index_New.csv data/tag_index.csv data/Table_Desc_2.csv "$SEARCH_TYPES" "$WORDS" "$FILTER" data/Table_with_Columns.csv
