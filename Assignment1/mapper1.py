#!/usr/bin/python3
import sys
import json
import math

def check(j_content, k):
	if not all(i.isalpha() or i.isspace() for i in j_content['word']):
		return False

	# country code must consist of two upper case letters
	if ((len(j_content['countrycode']) != 2) or (j_content['countrycode'] != (j_content['countrycode']).upper())):
		return False

	# All values must be either true or false
	if not ((j_content['recognized'] == True) or (j_content['recognized'] == False)):
		return False

	# Numeric string that's 16 characters long 
	if (len(j_content['key_id']) != 16) or (not j_content['key_id'].isnumeric()):
		return False

	# There must be at least one stroke
	if (len(j_content['drawing']) < 1):
		return False

	# Flag will tell us whether the data was inconsistent in any of the strokes
	flag = False
	for stroke in j_content['drawing']:
		# Two lists only to store x and y values
		if len(stroke) != 2:
			flag = True
			break

		# The lengths of the two lists must be equal
		if ((len(stroke[0]) != len(stroke[1]))):
			flag = True
			break
	# In case something went wrong, the data point must be skipped
	if (flag):
		return False

	dist = math.sqrt((j_content['drawing'][0][0][0])**2 + (j_content['drawing'][0][1][0])**2)
	if (dist <= k):
		return False
	return True



def main():
	word = sys.argv[1]
	k = sys.argv[2]
	k = int(k)
	for line in sys.stdin:
		line1 = line
		line = line.strip("{")
		line = line.strip("}")
		attribute = line.split(",")
		if ((attribute[3].replace('"','').split(": ")[1]!='false') and (attribute[3].replace('"','').split(": ")[1]!='true')):
			continue
		j_content = json.loads(line1.strip())
		if (j_content['word'] == word):
			if (check(j_content, k)):
				print(j_content['countrycode'], 1,sep="\t")

if __name__=="__main__":
	main()



