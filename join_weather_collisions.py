from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
import csv
import datetime
from dateutil.parser import parse

def map_weather_hours(idx, part):
    if idx == 0:
        part.next()
    for p in csv.reader(part):
        datehour= p[3].split(':')[0]
        yield (datehour, p)
        
def map_collision_hours(idx, part):
    if idx == 0:
        part.next()
    for p in csv.reader(part):
        datehour = str(parse(p[7])).split(':')[0]
        p[7] = str(parse(p[7]))
        yield (datehour, p)            

schema = StructType([StructField('BOROUGH', StringType()),
                    StructField('CONTRIBUTING_FACTOR_VEHICLE_1', StringType()),
                    StructField('CONTRIBUTING_FACTOR_VEHICLE_2', StringType()),
                    StructField('CONTRIBUTING_FACTOR_VEHICLE_3', StringType()),
                    StructField('CONTRIBUTING_FACTOR_VEHICLE_4', StringType()),
                    StructField('CONTRIBUTING_FACTOR_VEHICLE_5', StringType()),
                    StructField('CROSS_STREET_NAME', StringType()),
                    StructField('DATETIME', StringType()),
                    StructField('LATITUDE', StringType()),
                    StructField('LOCATION', StringType()),
                    StructField('LONGITUDE', StringType()),
                    StructField('NUMBER_OF_CYCLIST_INJURED', StringType()),
                    StructField('NUMBER_OF_CYCLIST_KILLED', StringType()),
                    StructField('NUMBER_OF_MOTORIST_INJURED', StringType()),
                    StructField('NUMBER_OF_MOTORIST_KILLED', StringType()),
                    StructField('NUMBER_OF_PEDESTRIANS_INJURED', StringType()),
                    StructField('NUMBER_OF_PEDESTRIANS_KILLED', StringType()),
                    StructField('NUMBER_OF_PERSONS_INJURED', StringType()),
                    StructField('NUMBER_OF_PERSONS_KILLED', StringType()),
                    StructField('OFF_STREET_NAME', StringType()),
                    StructField('ON_STREET_NAME', StringType()),
                    StructField('UNIQUE_KEY', StringType()),
                    StructField('VEHICLE_TYPE_CODE_1', StringType()),
                    StructField('VEHICLE_TYPE_CODE_2', StringType()),
                    StructField('VEHICLE_TYPE_CODE_3', StringType()),
                    StructField('VEHICLE_TYPE_CODE_4', StringType()),
                    StructField('VEHICLE_TYPE_CODE_5', StringType()),
                    StructField('ZIP_CODE', StringType()),
                    StructField('', StringType()),
                    StructField('Conditions', StringType()),
                    StructField('DateUTC', StringType()),
                    StructField('DatetimeEDT', StringType()),
                    StructField('Dew_PointF', StringType()),
                    StructField('Events', StringType()),
                    StructField('Freezing', StringType()),
                    StructField('Gust_SpeedMPH', StringType()),
                    StructField('Humidity', StringType()),
                    StructField('LowVisibility', StringType()),
                    StructField('PrecipitationIn', StringType()),
                    StructField('Rain', StringType()),
                    StructField('Sea_Level_PressureIn', StringType()),
                    StructField('Snow', StringType()),
                    StructField('TemperatureF', StringType()),
                    StructField('TimeEDT', StringType()),
                    StructField('VisibilityMPH', StringType()),
                    StructField('WindDirDegrees', StringType()),
                    StructField('Wind_Direction', StringType()),
                    StructField('Wind_SpeedMPH', StringType()),
                    StructField('Windy', StringType())])
        
if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    weather_data = sc.textFile('hdfs:///user/dzeng01/project/processed_2011-2017_fixed.csv')
    weather_data_rdd = weather_data.mapPartitionsWithIndex(map_weather_hours)
    weather_data_rdd = weather_data_rdd.groupByKey().mapValues(list)

    nypd_collisions = sc.textFile('hdfs:///user/dzeng01/project/coalesce_nypd_collisions_with_datetime.csv')
    nypd_collisions_rdd = nypd_collisions.mapPartitionsWithIndex(map_collision_hours)

    joined_data = nypd_collisions_rdd.join(weather_data_rdd)
    final_join=joined_data.map(lambda x: x[1][0] + x[1][1][0])
    final_join_df = sqlContext.createDataFrame(final_join, schema)
    sorted_final_join = final_join_df.sort('DATETIME')

    sorted_final_join.coalesce(1).write.csv('hdfs:///user/dzeng01/project/coalesce_joined_weather_collisions_sorted_fixed', header=True)