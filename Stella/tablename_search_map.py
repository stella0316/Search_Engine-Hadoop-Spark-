#!/usr/bin/env python

import os
import sys
import string
import csv

reader = csv.reader(sys.stdin,delimiter=',')
next(reader,None)
for line in reader:
	docid = line[0]
	table_name = line[1]
	line = table_name.strip()
	words = line.split()
	for word in words:
		word = word.lower()
		print('%s\t%s,%d' % (word,docid,1))

