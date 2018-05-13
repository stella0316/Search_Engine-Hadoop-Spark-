
module load python/gnu/3.4.4
module load spark/2.2.0

SEARCH_TYPES="$1"
WORDS="$2"
FILTER="$3"

spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
main.py final_data/title_index.txt final_data/column_index.txt final_data/content_index.txt \
final_data/Master_Index_New.csv final_data/tag_index.csv \
final_data/Table_Desc_2.csv "$SEARCH_TYPES" "$WORDS" "$FILTER" final_data/Table_with_Columns.csv
