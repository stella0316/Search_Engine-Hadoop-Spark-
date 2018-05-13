#!/usr/bin/env python

import os
import sys
import string
import csv
import re

reader = csv.reader(sys.stdin,delimiter=',')
next(reader,None)

for line in reader:
	docid = line[0]
	table_name = line[1]
	line = table_name.strip()
	words = line.split()
	for word in words:
		#remove punctuation and the begining and end of the string
		regex = re.compile('[%s]' % re.escape(string.punctuation))
		word = regex.sub(' ', word)
		if len(word) > 1:
        		word_list = word.split()
        		for w in word_list:
            			w = w.lower()
				if w != '' and w!= ' ':
					print('%s\t%s,%d' % (w,docid,1))
		else:
			if word != '' and word != ' ':
				word = word.lower()
				print('%s\t%s,%d' % (word,docid,1))

