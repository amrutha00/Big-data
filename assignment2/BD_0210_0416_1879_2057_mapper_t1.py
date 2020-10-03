#!/usr/bin/python3

import sys
for line in sys.stdin:
    line=line.strip()
    if len(line)==0:
        continue
        
    
    if line[0] == '#':
        continue
        
    words=line.split("\t")
      
    if len(words) == 2:
        #print('{0}\t{1}'.format(words[0],words[1]))
        print(words[0] + '\t' + words[1])
