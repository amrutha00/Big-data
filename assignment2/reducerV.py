#!/usr/bin/python3
import sys

D={}


path_to_v = sys.argv[1]

f = open(path_to_v, "w")

for line in sys.stdin:
    from_node,to_node=line.split("\t")

    from_node=int(from_node)
    to_node=int(to_node)    
    if from_node not in D:
        D[from_node]=1
    else:
        pass
  
for key in D.keys():
    #value.sort()
    f.write('{0}, {1}\n'.format(key,1))
f.close()

