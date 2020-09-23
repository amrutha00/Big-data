#!/usr/bin/python3
import sys

D={}

file=open("v.txt","w+")



for line in sys.stdin:
    line=line.strip()
    from_node,to_node=line.split("\t")
    if from_node.isnumeric():
        from_node=int(from_node)
    if to_node.isnumeric():
        to_node=int(to_node)    
    if from_node not in D:
        D[from_node]=[to_node]
    else:
        D[from_node].append(to_node)
    
for key,value in D.items():
    value.sort()
    print('{0}\t{1}'.format(key,value))

for key in D.keys():
    #value.sort()
    file.write('{0}\t{1}'.format(key,1)+'\n') 


file.close()
