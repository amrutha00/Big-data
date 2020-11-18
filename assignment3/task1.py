#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys

spark = SparkSession \
	.builder \
	.appName("Task 1") \
	.getOrCreate()

if (len(sys.argv) != 4):
	print("Wrong number of arguments")
	sys.exit(-1)

wordValue = sys.argv[1]
df = spark.read.load(sys.argv[3], format="csv", inferSchema="true", header="true")

wordRows = df.filter(df['word'] == wordValue).cache()

booTrue = False
booFalse = False

#if booTrue=True, already accounted for the case where the word is recognised.
#if booFalse=True, we have accounted for the case where the word is not recognised.

if (len(wordRows.head(1)) == 0):
	truecount = 1
	truesum = 0.00000
	falsesum = 0.00000
	falsecount = 1
	booTrue = True
	booFalse = True
a = []
if (not (booTrue and booFalse)):	#at least one is true, i.e, word is present in dataset
	counts = wordRows.groupBy("recognized").count()
	a = counts.collect()	#convert from df to list

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
elif (len(a) == 2):
	truecount = a[0][1]
	falsecount = a[1][1]

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

