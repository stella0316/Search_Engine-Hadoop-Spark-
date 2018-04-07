#!/usr/bin/env python

import sys
import string
from operator import itemgetter
current_key = None
key = None
value = list()
current_docid = None
result = dict()

for line in sys.stdin:
	line = line.strip()
	key, pair = line.split('\t',1)
	docid = int(pair[0])
	if current_key == key:
		value.append(docid)
	else:
		if current_key:
			total = len(set(value))
			result[current_key] = (total,list(set(value)))
			#print('%s\t%s\t%s' % (current_key,total,list(set(value))))
		current_key = key
		value = list()
		value.append(docid)

if current_key == key:
	total = len(set(value))
	result[current_key] = (total,list(set(value)))

sort_result = sorted(result.items(),key=itemgetter(1),reverse=True)
for f in sort_result:
	print('%s\t%s,%s' % (f[0],f[1][0],f[1][1]))
