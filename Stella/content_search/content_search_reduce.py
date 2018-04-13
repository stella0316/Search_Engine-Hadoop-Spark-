#!/usr/bin/env python


import sys
from operator import itemgetter

current_word = None
word = None
value = list()
result = dict()

for line in sys.stdin:
	line = line.strip()
	try:
		word, pair = line.split('\t',1)
		doc = pair.split(',',1)[0]
	except ValueError:
		continue

	if current_word == word:
		value.append(doc)
	else:
		if current_word:
			doc_list = [(v,value.count(v)) for v in set(value)]		
			total = len(doc_list)
			
			result[current_word] = (total,[x[0] for x in sorted(doc_list,key = itemgetter(1),reverse=True)])
		current_word = word
		value = list()
		value.append(doc)

if current_word == word:
	doc_list = [(v,value.count(v)) for v in set(value)]
	total = len(doc_list)
	result[current_word] = (total,[x[0] for x in sorted(doc_list,key = itemgetter(1),reverse=True)])

sort_result = sorted(result.items(),key = itemgetter(1), reverse = True)

for f in sort_result:
	print('%s\t%s,%s' % (f[0],f[1][0],f[1][1]))

		
