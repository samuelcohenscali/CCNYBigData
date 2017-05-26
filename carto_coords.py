from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import csv




        
if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    df = spark.read.load('hdfs:///user/dzeng01/project/joined_weather_collisions_sorted_fixed.csv', format='csv', header=True, inferSchema=False)
    df.cache()

    overall_df = df.filter(col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
        .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

    rain_df = df.filter(col('Rain').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
        .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

    snow_df = df.filter(col('Snow').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
        .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

    windy_df = df.filter(col('Windy').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
        .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

    low_visibility_df = df.filter(col('LowVisibility').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
        .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

    inclement_df = df.filter((col('Rain').like("1") | col('Snow').like("1") | col('Windy').like("1") | col('LowVisibility').like("1") | \
        col('Freezing').like("1")) & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE')\
        .groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)
        
    freezing_df = df.filter(col('Freezing').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0'))\
         .select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)


    overall_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/overall_coords", header=True)
    rain_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/rain_coords", header=True)
    snow_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/snow_coords", header=True)
    windy_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/windy_coords", header=True)
    inclement_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/inclement_coords", header=True)
    low_visibility_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/low_visibility_coords", header=True)
    freezing_df.coalesce(1).write.csv("hdfs:///user/dzeng01/project/total_coords_for_carto/freezing_coords", header=True)