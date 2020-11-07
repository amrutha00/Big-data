#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys

spark = SparkSession \
	.builder \
	.appName("Task 2") \
	.getOrCreate()

if (len(sys.argv) != 5):
	print("Wrong number of arguments")
	sys.exit(-1)

wordValue = sys.argv[1]
k = int(sys.argv[2])
df_shapestat= spark.read.load(sys.argv[4], format="csv", inferSchema="true", header="true")
df_shape= spark.read.load(sys.argv[3], format="csv", inferSchema="true", header="true")
#df_shapestat.printSchema()
#df_shape.printSchema()

shapestat=df_shapestat.select("word","recognized","Total_Strokes", "key_id")


wordRows = shapestat.filter(df_shapestat['word'] == wordValue)

if(len(wordRows.head(1)) == 0):
	print("0");


else:
	df = df_shape.join(wordRows, 'key_id', 'inner')
	df1=df.filter(df["recognized"]=="FALSE")
	df2=df1.filter(df1["Total_Strokes"]<k)
	df3=df2.groupBy("countrycode").count().orderBy("countrycode",ascending=True).collect()
	#print(df3)

	for i in range(0,len(df3)):
		if(df3[i][1]!=0):
			print(df3[i][0],(df3[i][1]),sep=",")