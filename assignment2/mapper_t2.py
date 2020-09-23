#!/usr/bin/python3
import sys



file=open("v.txt","r")
v_lines=file.readlines()


#D={}
#for v_line in v_lines:
#	v=v_line.strip()
#	v=v.split("\t")
#	D[v[0]]=int(v[1])

	

for line,v_line in sys.stdin,v_lines:
    line=line.strip()
    v=v_line.strip()
    v=v.split("\t")

    
    key,value=line.split("\t")
    value=value.strip("[")
    value=value.strip("]")
    value=value.split(",")
    
    contribution=int(v[1])/len(value)
    print(key,contribution)
    
file.close()