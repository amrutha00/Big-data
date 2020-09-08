#!/usr/bin/python3
import sys

#reducer1


sum_recognised=0
sum_unrecognised=0
try:
    for line in sys.stdin:
        line=line.strip(" ")
        word,count = line.split("\t")
        count=int(count)
        if word=="recognised":
        	sum_recognised = sum_recognised + count
        else:
        	sum_unrecognised=sum_unrecognised + count
    print(sum_recognised,sum_unrecognised,sep="\n")    
except:
     
    sys.exit()


        
