from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as sf
import csv

        
if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    df = spark.read.load('hdfs:///user/dzeng01/project/joined_weather_collisions_sorted_fixed.csv', format='csv', header=True, inferSchema=False)
    df.cache()

    overall_df = df.filter(col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)
    overall_df.cache()    
    

    rain_counts = df.select("LOCATION", 'LATITUDE', 'LONGITUDE', df.Rain.cast("integer").alias("Rain")).groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').agg(sf.sum('Rain').alias('rain_count')).orderBy('rain_count', ascending=False)
    joined_rain_counts = overall_df.join(rain_counts.select("LOCATION","rain_count"), "LOCATION")
    rain_percents = joined_rain_counts.select("*", ((joined_rain_counts['rain_count']/joined_rain_counts['count'])*100).alias("percent_rain")).orderBy(['percent_rain','count'], ascending=False)

    snow_counts = df.select("LOCATION", 'LATITUDE', 'LONGITUDE', df.Snow.cast("integer").alias("snow")).groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').agg(sf.sum('snow').alias('snow_count')).orderBy('snow_count', ascending=False)
    joined_snow_counts = overall_df.join(snow_counts.select("LOCATION","snow_count"), "LOCATION")
    snow_percents = joined_snow_counts.select("*", ((joined_snow_counts['snow_count']/joined_snow_counts['count'])*100).alias("percent_snow")).orderBy(['percent_snow','count'], ascending=False)

    windy_counts = df.select("LOCATION", 'LATITUDE', 'LONGITUDE', df.Windy.cast("integer").alias("windy")).groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').agg(sf.sum('windy').alias('windy_count')).orderBy('windy_count', ascending=False)
    joined_windy_counts = overall_df.join(windy_counts.select("LOCATION","windy_count"), "LOCATION")
    windy_percents = joined_windy_counts.select("*", ((joined_windy_counts['windy_count']/joined_windy_counts['count'])*100).alias("percent_windy")).orderBy(['percent_windy','count'], ascending=False)

    low_visibility_counts = df.select("LOCATION", 'LATITUDE', 'LONGITUDE', df.LowVisibility.cast("integer").alias("low_visibility")).groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').agg(sf.sum('low_visibility').alias('low_visibility_count')).orderBy('low_visibility_count', ascending=False)
    joined_low_visibility_counts = overall_df.join(low_visibility_counts.select("LOCATION","low_visibility_count"), "LOCATION")
    low_visibility_percents = joined_low_visibility_counts.select("*", ((joined_low_visibility_counts['low_visibility_count']/joined_low_visibility_counts['count'])*100).alias("percent_low_visibility")).orderBy(['percent_low_visibility','count'], ascending=False)

    freezing_counts = df.select("LOCATION", 'LATITUDE', 'LONGITUDE', df.Freezing.cast("integer").alias("freezing")).groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').agg(sf.sum('freezing').alias('freezing_count')).orderBy('freezing_count', ascending=False)
    joined_freezing_counts = overall_df.join(freezing_counts.select("LOCATION","freezing_count"), "LOCATION")
    freezing_percents = joined_freezing_counts.select("*", ((joined_freezing_counts['freezing_count']/joined_freezing_counts['count'])*100).alias("percent_freezing")).orderBy(['percent_freezing','count'], ascending=False)


    overall_join = overall_df.join(rain_percents.select("LOCATION","rain_count", "percent_rain"), "LOCATION")
    overall_join = overall_join.join(snow_percents.select("LOCATION","snow_count", "percent_snow"), "LOCATION")
    overall_join = overall_join.join(windy_percents.select("LOCATION","windy_count", "percent_windy"), "LOCATION")
    overall_join = overall_join.join(low_visibility_percents.select("LOCATION","low_visibility_count", "percent_low_visibility"), "LOCATION")
    overall_join = overall_join.join(freezing_percents.select("LOCATION","freezing_count", "percent_freezing"), "LOCATION")
    
    overall_join.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/overall_coords_normalized", header=True)
    

    rain_percents_filtered = rain_percents.filter(col('rain_count') > 0)
    snow_percents_filtered = snow_percents.filter(col('snow_count') > 0)
    windy_percents_filtered = windy_percents.filter(col('windy_count') > 0)
    low_visibility_percents_filtered = low_visibility_percents.filter(col('low_visibility_count') > 0)
    freezing_percents_filtered = freezing_percents.filter(col('freezing_count') > 0)

    rain_percents_filtered.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/rain_coords_normalized", header=True)
    snow_percents_filtered.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/snow_coords_normalized", header=True)
    windy_percents_filtered.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/windy_coords_normalized", header=True)
    low_visibility_percents_filtered.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/low_visibility_coords_normalized", header=True)
    freezing_percents_filtered.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto_normalized/freezing_coords_normalized", header=True)
