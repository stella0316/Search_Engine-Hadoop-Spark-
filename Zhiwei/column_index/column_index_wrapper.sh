#!/bin/bash

OUTFILE1="tfidf_column.out"
OUTFILE2="tfidf_index.out"

if [ -e "$OUTFILE1" ]
then
rm -r "$OUTFILE1"
fi

if [ -e "$OUTFILE2" ]
then
rm -r "$OUTFILE2"
fi

spark-submit column_index.py Table_with_Columns.csv Master_Index_New.csv
