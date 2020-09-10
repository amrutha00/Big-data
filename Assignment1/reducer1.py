#!/usr/bin/python3
import sys

# Current word will hold the value of country
current_word = None
# Current count will tell us the count of the current_country seen thus far
current_count = 0
word = None
for line in sys.stdin:
	line = line.strip()
	word,count = line.split("\t")
	count = int(count)

	# If the word scanned is equal to current_word, we need to update current_count 
	if (current_word == word):
		current_count += count
	else:

		# This condition is almost always true, except in the very first iteration when current_word is None because 
		# it was never assigned a value
		if current_word:
			print(current_word, current_count,sep=",")
		current_count = count
		current_word = word
# The last word doesnt get printed in the for loop, and hence needs to be printed here
if current_word == word:
	print(current_word, current_count,sep=",")


