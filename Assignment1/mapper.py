import json

def load_data():
	"""
		This function would load data from the ndjson file, clean the data and return a list of dictionaries
	"""
	with open('plane_carriers.ndjson') as f:
		ans = []
		for line in f:
			#j_content is a dictionary mapping JSON properties with their values
			j_content = json.loads(line)

			# All characters must be either alphabets or whitespaces, otherwise skip
			if not all(i.isalpha() or i.isspace() for i in j_content['word']):
				continue

			# country code must consist of two upper case letters
			if ((len(j_content['countrycode']) != 2) or (j_content['countrycode'] != (j_content['countrycode']).upper())):
				continue

			# All values must be either true or false
			if not ((j_content['recognized'] == True) or (j_content['recognized'] == False)):
				continue

			# Numeric string that's 16 characters long 
			if (len(j_content['key_id']) != 16) or (not j_content['key_id'].isnumeric()):
				continue

			# There must be at least one stroke
			if (len(j_content['drawing']) < 1):
				continue

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
				continue

			# If everything is alright
			ans.append(j_content)

		# ans is a list of dictionaries - (Dont know what this must return - this can change)
		return ans

def mapper():
	raise NotImplementedError

def main():
	raise NotImplementedError

if __name__=="__main__":
	main()	

