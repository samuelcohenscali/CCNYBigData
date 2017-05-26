from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

'''
This script is to filter out certain conditions and the NYPD contributing factors for each.
The results are collected from STDOUT on the NYU CUSP Cluster.
'''

if __name__ == '__main__':
	sc = SparkContext()
	spark = SparkSession(sc)
	sqlContext = SQLContext(sc)

	path = "hdfs:///user/dzeng01/project/joined_weather_collisions_sorted_fixed.csv"
	df = spark.read.load(path, format='csv', header=True, inferSchema=False)
	df.cache()

	print("SNOW")
	df.filter(col('Snow').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('Snow').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)


	print("RAIN")
	df.filter(col('Rain').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('Rain').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("WIND")
	df.filter(col('Windy').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('Windy').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("FREEZE")
	df.filter(col('Freezing').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('Freezing').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)


	print("FOG")
	df.filter(col('LowVisibility').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('LowVisibility').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("Inclement")
	df.filter(col('Snow').like("1") | col('Rain').like("1") | col('Windy').like("1") | col('Freezing').like("1") | col('LowVisibility').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(col('Snow').like("1") | col('Rain').like("1") | col('Windy').like("1") | col('Freezing').like("1") | col('LowVisibility').like("1")).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("Overall")
	df.groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("Injury")
	df.filter(~col("NUMBER_OF_CYCLIST_INJURED").like('0') | ~col("NUMBER_OF_MOTORIST_INJURED").like('0') | ~col("NUMBER_OF_PEDESTRIANS_INJURED").like('0')).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(~col("NUMBER_OF_CYCLIST_INJURED").like('0') | ~col("NUMBER_OF_MOTORIST_INJURED").like('0') | ~col("NUMBER_OF_PEDESTRIANS_INJURED").like('0')).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

	print("KILLED")
	df.filter(~col("NUMBER_OF_CYCLIST_KILLED").like('0') | ~col("NUMBER_OF_MOTORIST_KILLED").like('0') | ~col("NUMBER_OF_PEDESTRIANS_KILLED").like('0')).groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().orderBy('count', ascending=False).show(truncate=False)
	df.filter(~col("NUMBER_OF_CYCLIST_KILLED").like('0') | ~col("NUMBER_OF_MOTORIST_KILLED").like('0') | ~col("NUMBER_OF_PEDESTRIANS_KILLED").like('0')).groupBy("CONTRIBUTING_FACTOR_VEHICLE_2").count().orderBy('count', ascending=False).show(truncate=False)

