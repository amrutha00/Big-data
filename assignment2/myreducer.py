#!/usr/bin/python3
import sys



temp = []
prev_node = None
for line in sys.stdin:
    from_node,to_node=line.split("\t")

    from_node=int(from_node)
    to_node=int(to_node)
    if (prev_node == from_node):
    	temp.append(to_node)
    else:
    	if (prev_node is not None):
    		print(prev_node, temp, sep="\t")
    	prev_node = from_node
    	temp = [to_node]
print(prev_node, temp, sep="\t")
