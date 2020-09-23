#!/usr/bin/python3
import sys


#print('{0}\t{1}'.format(words[0],words[1]))

for line in sys.stdin:
    line=line.strip()
    words=line.split("\t")
    # words[0] = from_node_id
    # words[1] = to_node_id
    if len(words) == 2:
        print('{0}\t{1}'.format(words[0],words[1]))
    else:
        print('{0}'.format(words[0]))
