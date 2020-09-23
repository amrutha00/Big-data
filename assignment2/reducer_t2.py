#!/usr/bin/python3
import sys

D={}

#file=open("v1","w+")



for line in sys.stdin:
    line=line.strip()
    
    node,value=line.split("\t")
    if node not in D:
        D[node]=float(value)
    else:
        D[node]=D[node]+float(value)
#print(D)
for key in D.keys():
    new_pagerank= (0.15  + 0.85*D[key])
    print(key,new_pagerank,sep="\t")
    #fwrite('{0}, {1}'.format(key,new_pagerank)+'\n')
#file.close()
