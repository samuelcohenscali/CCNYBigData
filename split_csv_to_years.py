from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
import csv
import datetime
from dateutil.parser import parse

'''
This script is responsible for splitting the main joined weather data 
into years for partitioned processing (as well as to reduce overhead 
on both local and cluster memory requirements)
'''

def filter_year(x):
    return x.split("-")[0]

        
if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)
    df = spark.read.load('hdfs:///user/dzeng01/project/joined_weather_collisions_sorted_fixed.csv', format='csv', header=True, inferSchema=False)
    df.registerTempTable('temp')

    year_filter = udf(filter_year, StringType())
    newcol_df = df.withColumn("YEAR", year_filter("DATETIME"))
    unique_years = newcol_df.select("YEAR").distinct()
    unique_years_list = unique_years.rdd.flatMap(lambda x: x).collect()

    for year in unique_years_list:
        year_wildcard = year+"%"
        query = 'select * from temp where DATETIME LIKE "{0}"'.format(year_wildcard)
        temp = spark.sql(query)
        temp.coalesce(1).write.csv('hdfs:///user/dzeng01/project/partitioned_joins_by_year/joins_in_'+year, header=True)