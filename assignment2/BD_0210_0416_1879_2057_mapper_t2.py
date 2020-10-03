#!/usr/bin/python3

import sys
path_to_v = sys.argv[1]
f = open(path_to_v, "r")

pgrank = dict()
l_nodes= dict()
for line in f:
	line = line.strip()
	node, pagerank = line.split(",")
	l_nodes[node] = 1
	pgrank[node] = float(pagerank)
f.close()

for line in sys.stdin:
	line = line.strip()
	#from_node, to_nodes = line.split("\t")
	try:
		from_node,to_nodes=line.split("\t")
	except:
		from_node=line.strip("\t")
		to_nodes=-1
	if (to_nodes == -1):
		continue
	to_nodes = to_nodes.split()
	length = len(to_nodes)
	for i in to_nodes:
		if i in l_nodes:
			l_nodes[i] = 0
			#print("{0}\t{1}".format(i, pgrank[from_node]/length))
			print(i + '\t' + str(pgrank[from_node]/length))
						
l_nodes_keys = l_nodes.keys()

for i in l_nodes_keys:
	#print("{0}\t{1}".format(i, 0.00000))
	print(i + '\t0.00000') 
