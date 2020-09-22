#!/usr/bin/python3
import sys


#print('{0}\t{1}'.format(words[0],words[1]))

for line in sys.stdin:
    line=line.strip()
    words=line.split("\t")
    print('{0}\t{1}'.format(words[0],words[1]))