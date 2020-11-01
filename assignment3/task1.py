#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys

# SparkContext
spark = SparkSession \
	.builder \
	.appName("Task 1") \
	.getOrCreate()

# Checking for the number of arguments
if (len(sys.argv) != 4):
	print("Wrong number of arguments")
	sys.exit(-1)

# First command line argument = the word value
wordValue = sys.argv[1]
df = spark.read.load(sys.argv[2], format="csv", inferSchema="true", header="true")

# wordRows contains all the rows that has the given word
wordRows = df.filter(df['word'] == wordValue)

# When booTrue is true, its sum and count value don't have to be computed
# Similarly for booFalse
booTrue = False
booFalse = False

# When the word doesn't exist in the dataset, update sum and count values for both true and false
if (len(wordRows.head(1)) == 0):
	truecount = 1
	truesum = 0.00000
	falsesum = 0.00000
	falsecount = 1
	booTrue = True
	booFalse = True


a = []

# When the word does exist in the dataset
if (not (booTrue and booFalse)):
	counts = wordRows.groupBy("recognized").count()
	a = counts.collect()

# If all the rows containing the word have the same recognized value (either true or false), update accordingly
if (len(a) == 1):
	if (a[0][0] == True):
		falsesum = 0.00000
		falsecount = 1
		booFalse = True
		truecount = a[0][1]
	else:
		truecount = 1
		truesum = 0.00000
		booTrue = True
		falsecount = a[0][1]

# If all the rows containing the word have both true and false values in dataset under recognized column
elif (len(a) == 2):
	truecount = a[0][1]
	falsecount = a[1][1]

# Else, the word doesn't exist, and we've already taken care of this case (line 42)

if (not booTrue):
	truerows = wordRows.filter(df['recognized'] == "true")
	truesum = truerows.groupBy().sum('Total_Strokes').collect()[0][0]
if (not booFalse):
	falserows = wordRows.filter(df['recognized'] == "false")
	falsesum = falserows.groupBy().sum('Total_Strokes').collect()[0][0]


# falsesum = falserows.select(F.sum('Total_Strokes')).collect()[0][0]

trueavg = float(truesum)/float(truecount)
falseavg = float(falsesum)/float(falsecount)

#print(round(trueavg, 5))
#print(round(falseavg, 5))

print("{0:.5f}".format(round(trueavg,5)))
print("{0:.5f}".format(round(falseavg,5)))

