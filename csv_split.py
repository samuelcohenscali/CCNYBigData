from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)


    for yr in range(12,18):
        path = "hdfs:///user/dzeng01/project/partitioned_joins_by_year/joins_in_20{0}.csv".format(yr)
        df = spark.read.load(path, format='csv', header=True, inferSchema=False)
        df.cache()
        sqlContext.registerDataFrameAsTable(df, 'df')

        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)

            if df.filter(col('DATETIME').like(date)).count() == 0: 
                continue
            df.filter(col('DATETIME').like(date)).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1], header=True)

            df.filter(col('DATETIME').like(date) & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1]+"_geom", header=True)

            df.filter(col('DATETIME').like(date) & col('Rain').like("1")).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_rain", header=True)
            df.filter(col('DATETIME').like(date) & col('Rain').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_geom_rain", header=True)

            df.filter(col('DATETIME').like(date) & col('Snow').like("1")).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_snow", header=True)
            df.filter(col('DATETIME').like(date) & col('Snow').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_snow_geom", header=True)

            df.filter(col('DATETIME').like(date) & col('Windy').like("1")).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_wind", header=True)
            df.filter(col('DATETIME').like(date) & col('Windy').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_wind_geom", header=True)

            df.filter(col('DATETIME').like(date) & col('LowVisibility').like("1")).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_fog", header=True)
            df.filter(col('DATETIME').like(date) & col('LowVisibility').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_fog_geom", header=True)

            df.filter(col('DATETIME').like(date) & col('Freezing').like("1")).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_ice", header=True)
            df.filter(col('DATETIME').like(date) & col('Freezing').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_ice_geom", header=True)

            df.filter(col('DATETIME').like(date) & (col('Rain').like("1") | col('Snow').like("1") | col('Windy').like("1") | col('LowVisibility').like("1") | col('Freezing').like("1"))).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_inclement", header=True)
            df.filter(col('DATETIME').like(date) & (col('Rain').like("1") | col('Snow').like("1") | col('Windy').like("1") | col('LowVisibility').like("1") | col('Freezing').like("1")) & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).coalesce(1).write.csv("hdfs:///user/dzeng01/project/carl_folder/" + date[:-1] + "/" + date[:-1] + "_inclement_geom", header=True)
