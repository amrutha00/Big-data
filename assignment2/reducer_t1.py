#!/usr/bin/python3
import sys

D={}

file=open("v","w+")



for line in sys.stdin:
    line=line.strip()
    try:
        from_node,to_node=line.split("\t")
    except:
        from_node=line.strip("\t")
        to_node=-1
    """    
    if from_node.isnumeric():
        from_node=int(from_node)
    if to_node.isnumeric():
        to_node=int(to_node)    
    """
    if from_node not in D:
        if to_node==-1:
            D[from_node]=[]
        else:
            D[from_node]=[to_node]
    else:
        D[from_node].append(to_node)
    
for key,value in D.items():
    value.sort()
    print('{0}\t{1}'.format(key,value))
    file.write('{0}\t{1}'.format(key,1)+'\n')


     


file.close()
