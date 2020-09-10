#!/usr/bin/python3
import sys

#reducer1


# sum_recognised=0
# sum_unrecognised=0
# try:
#     for line in sys.stdin:
#         line=line.strip(" ")
#         word,count = line.split("\t")
#         count=int(count)
#         if word=="recognised":
#         	sum_recognised = sum_recognised + count
#         else:
#         	sum_unrecognised=sum_unrecognised + count
#     print(sum_recognised,sum_unrecognised,sep="\n")    
# except:
     
#     sys.exit()

# Because our mapper prints recognized and unrecognized, and that the partitioning function sorts the keys, recognized count 
# would be printed before unrecognized count, which is the required output format anyway.

# current_word will be used to keep track of either recognized or unrecognized tuples
current_word = None
# current_count will contain the number of tuples of word = current_word seen till now
current_count = 0
word = None
for line in sys.stdin:
    line = line.strip()
    word,count = line.split("\t")
    count = int(count)
    # If the seen word is equal to the one we're counting, update count
    if (current_word == word):
        current_count += count
    else:
        if current_word:
            print(current_count)
        # This is a new word, so initialise current_word and current_count variables 
        current_count = count
        current_word = word
if current_word == word:
    print(current_count)

        
