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
df_shapestat.printSchema()
df_shape.printSchema()

shapestat=df_shapestat.select("word","recognized","Total_Strokes", "key_id")
# shape=df_shape.drop("key_id")

wordRows = shapestat.filter(df_shapestat['word'] == wordValue)
# wordRows = wordRow.withColumnRenamed("word","key")
if(len(wordRows.head(1)) == 0):
	pass


else:
	# condition=[shape.word== wordRows.key]
	# df_joined=shape.join(wordRows,condition,'inner')
	df_joined = df_shape.join(wordRows, 'key_id', 'inner')
	#df_joined.printSchema()
	#df=df_joined.select(df_joined.key,df_joined.countrycode,df_joined.recognized,df_joined.Total_Strokes)
	df=df_joined.drop("key")
	df1=df.filter(df_joined["recognized"]=="FALSE")
	#print(df1)
	df2=df1.filter(df1["Total_Strokes"]<k)
	#print(df2)
	df3=df2.groupBy("countrycode").count().orderBy("countrycode",ascending=True).collect()
	print(df3)

	for i in range(0,len(df3)):
		print(df3[i][0],(df3[i][1]),sep=",")

