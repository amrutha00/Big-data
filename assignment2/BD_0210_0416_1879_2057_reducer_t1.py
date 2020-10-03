#!/usr/bin/python3

import sys
D={}

path_to_v = sys.argv[1]

file=open(path_to_v,"w+")

prev_from_node = None

last_printed_to_v = prev_from_node

for line in sys.stdin:
    line = line.strip()
    try:
        from_node, node = line.split("\t")
    except:
        continue
    if (from_node == prev_from_node):
        print(node, end=" ")
    else:
        # This is true when it's not the first iteration
        if (prev_from_node is not None):
            print()         
            last_printed_to_v = prev_from_node
        print(from_node,end="\t")
        print(node,end=" ")
        
        prev_from_node = from_node
        #file.write("{0}, {1}".format(prev_from_node, 1) + "\n")
        file.write(prev_from_node + ', 1\n') 

file.close()
