import sys
import ast

path_to_v = sys.argv[1]
f = open(path_to_v, "r")

pgrank = dict()

for line in f:
	line = line.strip()
	node, pagerank = line.split(", ")
	pgrank[node] = float(pagerank)
f.close()

for line in sys.stdin:
	line = line.strip()
	from_node, to_nodes = line.split("\t")
	to_nodes = ast.literal_eval(to_nodes)
	to_nodes = [n.strip() for n in to_nodes]
	n = len(to_nodes)
	for i in to_nodes:
		print(i, round(pgrank[from_node]/float(n), 5), sep="\t")
	
