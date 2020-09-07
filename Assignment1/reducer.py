import sys

#reducer1


sum_count=0
try:
    for line in sys.stdin:
        line=line.strip(" ")
        word, count = line.split("\t")
        count=int(count)
        sum_count = sum_count + count
except:
    print(sum_count) 
    sys.exit()


        
