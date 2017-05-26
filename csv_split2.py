from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

'''
Utility script. Simply used to split up CSV's on CUSP.
'''

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)


    for yr in range(12,18):

        date = "20{0}%".format(yr)
        df.filter(col('DATETIME').like(date) & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/" + date[:-1]+"_geom", header=True)


