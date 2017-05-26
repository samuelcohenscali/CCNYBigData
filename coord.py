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

		overall_df = df.filter(col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		rain_df = df.filter(col('Rain').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		snow_df = df.filter(col('Snow').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		windy_df = df.filter(col('Windy').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		inclement_df = df.filter((col('Rain').like("1") | col('Snow').like("1") | col('Windy').like("1") | col('LowVisibility').like("1") | col('Freezing').like("1")) & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		ow_visibility_df = df.filter(col('LowVisibility').like("1") & col('LATITUDE').isNotNull() & ~col('LATITUDE').like('0')).select("LOCATION", 'LATITUDE', 'LONGITUDE').groupBy("LOCATION", 'LATITUDE', 'LONGITUDE').count().orderBy('count', ascending=False)

		dest = "hdfs:///user/dzeng01/project/"

		overall_df.coalesce(1).write.csv(dest + "20{0}_coords_for_carto/2015_overall_coords".format(yr), header=True)
		rain_df.coalesce(1).write.csv(dest + "20{0}_coords_for_carto/2015_rain_coords".format(yr), header=True)
		snow_df.coalesce(1).write.csv(dest + "20{0}_coords_for_carto/2015_snow_coords".format(yr), header=True)
		windy_df.coalesce(1).write.csv(dest + "20{0}_coords_for_carto/2015_windy_coords".format(yr), header=True)
		inclement_df.coalesce(1).write.csv(dest + "20{0}_coords_for_carto/2015_inclement_coords".format(yr), header=True)