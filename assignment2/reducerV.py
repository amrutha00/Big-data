#!/usr/bin/python3
import sys

D={}




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
    print('{0}\t{1}'.format(key,1))
