#!/usr/bin/python3

import sys
prev_node = None
cont_sum = 0

for line in sys.stdin:
	line = line.strip()
	node,value = line.split("\t")
	if (node == prev_node):
		cont_sum += float(value)
	else:
		if (prev_node is not None):
			new_pagerank = ((0.85*cont_sum) + 0.15)
			print(prev_node, "%.5f"%round(new_pagerank, 5), sep=", ")
		prev_node = node
		cont_sum = float(value)
if (prev_node == node):
	new_pagerank = ((0.85*cont_sum) + 0.15)
	print(prev_node, "%.5f"%round(new_pagerank, 5), sep=", ")
