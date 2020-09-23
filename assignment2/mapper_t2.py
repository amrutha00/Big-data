#!/usr/bin/python3
import sys


#print('{0}\t{1}'.format(words[0],words[1]))
file=open("v.txt","r")
v_lines=file.readlines()
#print(v_lines)

D={}
for v_line in v_lines:
	v=v_line.strip()
	v=v.split("\t")
	D[v[0]]=int(v[1])

	

for line in sys.stdin:
    line=line.strip()
    
    key,value=line.split("\t")
    value=value.strip("[")
    value=value.strip("]")
    value=value.split(",")
    
    contribution=D[key]/len(value)
    print(key,contribution)
    
file.close()