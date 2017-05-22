# PySpark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
# Utility imports
from dateutil.parser import parse
import csv
import pyproj
import math

def fn(idx, part):
    if idx == 0:
        part.next()
    for r in csv.reader(part):
        datetime = r[0] + " " + r[1] + ":00"

        yield Row(
            DATETIME = datetime,
            BOROUGH = r[2],
            ZIP_CODE = r[3],
            LATITUDE = r[4],
            LONGITUDE = r[5],
            LOCATION = r[6],
            ON_STREET_NAME = r[7],
            CROSS_STREET_NAME = r[8],
            OFF_STREET_NAME = r[9],
            NUMBER_OF_PERSONS_INJURED = r[10],
            NUMBER_OF_PERSONS_KILLED = r[11],
            NUMBER_OF_PEDESTRIANS_INJURED = r[12],
            NUMBER_OF_PEDESTRIANS_KILLED = r[13],
            NUMBER_OF_CYCLIST_INJURED = r[14],
            NUMBER_OF_CYCLIST_KILLED = r[15],
            NUMBER_OF_MOTORIST_INJURED = r[16],
            NUMBER_OF_MOTORIST_KILLED = r[17],
            CONTRIBUTING_FACTOR_VEHICLE_1 = r[18],
            CONTRIBUTING_FACTOR_VEHICLE_2 = r[19],
            CONTRIBUTING_FACTOR_VEHICLE_3 = r[20],
            CONTRIBUTING_FACTOR_VEHICLE_4 = r[21],
            CONTRIBUTING_FACTOR_VEHICLE_5 = r[22],
            UNIQUE_KEY = r[23],
            VEHICLE_TYPE_CODE_1 = r[24],
            VEHICLE_TYPE_CODE_2 = r[25],
            VEHICLE_TYPE_CODE_3 = r[26],
            VEHICLE_TYPE_CODE_4 = r[27],
            VEHICLE_TYPE_CODE_5 = r[28]
            )



if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    # Gather Citibike Data
    rows = sc.textFile('hdfs:///user/dzeng01/project/NYPD_Motor_Vehicle_Collisions.csv')

    newrdd = rows.mapPartitionsWithIndex(fn)


    rddDataFrame = sqlContext.createDataFrame(newrdd)
    rddDataFrame.write.csv('hdfs:///user/ccolena000/nypd.csv', header=True)
