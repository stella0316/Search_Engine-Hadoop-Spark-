#!/bin/bash

OUTFILE="column_index.out"

if [ -e "$OUTFILE" ]
then
rm -r "$OUTFILE"
fi

spark-submit column_index.py Table_Describe.csv
