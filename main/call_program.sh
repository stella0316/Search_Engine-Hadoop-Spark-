
module load python/gnu/3.4.4
module load spark/2.2.0

SEARCH_TYPES="$1"
WORDS="$2"
FILTER="$3"

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
main.py search/title_index.txt search/column_index.txt search/content_index.txt search/Master_Index_New.csv search/tag_index.csv search/Table_Desc_2.csv "$SEARCH_TYPES" "$WORDS" "$FILTER" search/Table_with_Columns.csv
