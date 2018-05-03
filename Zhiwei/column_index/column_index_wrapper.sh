#!/bin/bash

OUTFILE="tfidf_column.out"

if [ -e "$OUTFILE" ]
then
rm -r "$OUTFILE"
fi

spark-submit column_index.py Table_with_Columns.csv Master_Index_New.csv
