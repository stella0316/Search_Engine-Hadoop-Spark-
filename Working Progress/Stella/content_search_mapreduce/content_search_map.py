#!/usr/bin/env python


import os
import sys
import string
import csv
import re

reader = csv.reader(sys.stdin,delimiter=',')
next(reader,None)

for line in reader:
	filepath = os.environ.get('mapreduce_map_input_file')
	file_name = os.path.split(filepath)[-1]
	
	for word in line:
		w = word.strip(string.punctuation)
		if w != '' and w != ' ':
			print('%s\t%s,%s' % (w.lower(),file_name,1))	
