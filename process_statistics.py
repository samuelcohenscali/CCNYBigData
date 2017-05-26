from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from calendar import monthrange
import json

'''
This script handles the main bulk of statistical data analysis of the joined dataset.
This script goes through the entire dataset to extract key metrics that give us 
valuable information on the statistical significance of particular inclement weather
conditions and the collision, injury, and death rates per month and year.
'''

# Months function used for JSON output.
def months(x):
    month = {1:"January",
             2:"Feburary",
             3:"March",
             4:"April",
             5:"May",
             6:"June",
             7:"July",
             8:"August",
             9:"September",
             10:"October",
             11:"November",
             12:"December"}
    return str(month[x])


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)


    for yr in range(12,18):
        path = "hdfs:///user/dzeng01/project/partitioned_joins_by_year/joins_in_20{0}.csv".format(yr)
        df = spark.read.load(path, format='csv', header=True, inferSchema=False)
        df.cache()
        sqlContext.registerDataFrameAsTable(df, 'df')
        
        inj_kil = ["NUMBER_OF_CYCLIST_INJURED",
        "NUMBER_OF_CYCLIST_KILLED",
        "NUMBER_OF_MOTORIST_INJURED",
        "NUMBER_OF_MOTORIST_KILLED",
        "NUMBER_OF_PEDESTRIANS_INJURED",
        "NUMBER_OF_PEDESTRIANS_KILLED",
        "NUMBER_OF_PERSONS_INJURED",
        "NUMBER_OF_PERSONS_KILLED"]

        for x in inj_kil:
            df = df.withColumn(x, df[x].cast(IntegerType()))




        #### This should be code that will read in each csv for each year and register as df.
        stats = {}
        stats["0_YEAR"] = int(str(20)+str(yr))   


        ## Overall Statistics
        stats['Overall'] = {}
        m_arr = [] # Overall Collision Data
        mi_arr = [] # Overall Collision Injury Data
        md_arr = [] # Overall Collision Death Data

        """PREPROCESSING"""
        # Overall Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            m_arr.append((x, sq.count())) 
            sq.unpersist()

        total = sum([x[1] for x in m_arr])

        # Overall Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            mi_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()

        injury_total = sum([x[1] for x in mi_arr])
        injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in mi_arr])

        # Overall Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            md_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        death_total = sum([x[1] for x in md_arr])
        death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in md_arr])







        # Statistics Deriving
        stats['Overall']['Total Number of Persons Injured in Year'] = injury_persons_total
        stats['Overall']['Total Number of Persons Injured Per Month'] = [str(
                                                                    (months(x+1), sum(map(lambda a: a[0]*a[1], mi_arr[x][2]['Cyclist'])) + 
                                                                                  sum(map(lambda a: a[0]*a[1], mi_arr[x][2]['Motorist'])) + 
                                                                                  sum(map(lambda a: a[0]*a[1], mi_arr[x][2]['Pedestrians'])))
                                                                    ) for x in range(0,12)]
        stats['Overall']['Total Number of Persons Killed in Year'] = death_persons_total
        stats['Overall']['Total Number of Persons Killed Per Month'] = [str(
                                                                    (months(x+1), sum(map(lambda a: a[0]*a[1], md_arr[x][2]['Cyclist'])) + 
                                                                                  sum(map(lambda a: a[0]*a[1], md_arr[x][2]['Motorist'])) + 
                                                                                  sum(map(lambda a: a[0]*a[1], md_arr[x][2]['Pedestrians'])))
                                                                    ) for x in range(0,12)]
        stats['Overall']['Total Number of Collisions in Year'] = total
        stats['Overall']['Total Number of Collisions w/ Injury in Year'] = injury_total
        stats['Overall']['Total Number of Collisions w/ Death in Year'] = death_total
        stats['Overall']['Average Collisions Per Month'] = total // 12
        stats['Overall']['Average Collisions Per Day'] = total // 365
        stats['Overall']['Monthly Adjusted Average Collision Weight (%)'] = [str(
                                                                    (months(x+1), round((float(m_arr[x][1])/float(total)*100.0),2))
                                                                    ) for x in range(0,12)]
        stats['Overall']['Collisions Per Month'] = [str(
                                                    (months(x+1), m_arr[x][1])
                                                    ) for x in range(0,12)]
        stats['Overall']['Average Collisions Per Day (Monthly)'] = [str(
                                                                    (months(x+1), m_arr[x][1]/monthrange(int(str(20)+str(yr)), x+1)[1])
                                                                    ) for x in range(0,12)]
        stats['Overall']['Injury Rate (Year %)'] = float(injury_total)/float(total) * 100.00
        stats['Overall']['Death Rate (Year %)'] = float(death_total)/float(total) * 100.00
        stats['Overall']['Number of Collisions w/ Injuries Per Month'] = [str(
                                                                        (months(x+1), mi_arr[x][1])
                                                                        ) for x in range(0,12)]
        stats['Overall']['Number of Collisions w/ Deaths Per Month'] = [str(
                                                                        (months(x+1), md_arr[x][1])
                                                                        ) for x in range(0,12)]
        stats['Overall']['Injury Collision Rate Per Month (Month %)'] = [str(
                                                            (months(x+1), round((float(mi_arr[x][1])/float(m_arr[x][1]) * 100.00),2))
                                                            ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Overall']['Death Collision Rate Per Month (Month %)'] = [str(
                                                            (months(x+1), round((float(md_arr[x][1])/float(m_arr[x][1]) * 100.00),2))
                                                            ) for x in range(0,12) if m_arr[x][1] != 0]






        # Rain Statistics
        stats['Rain'] = {}
        r_arr = [] # Rain Collision Data
        ri_arr = [] # Rain Collision Injury Data
        rd_arr = [] # Rain Collision Death Data

        """PREPROCESSING"""
        # Rain Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Rain="1" and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            r_arr.append((x, sq.count()))
            sq.unpersist()
        rain_total = sum([x[1] for x in r_arr])

        # Rain Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Rain="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            ri_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        rain_injury_total = sum([x[1] for x in ri_arr])
        rain_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in ri_arr])

        # Rain Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Rain="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            rd_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        rain_death_total = sum([x[1] for x in rd_arr])
        rain_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in rd_arr])




        # Statistics Deriving
        stats['Rain']['Total Number of Persons Injured in Collision w/ Rain'] = rain_injury_persons_total
        stats['Rain']['Total Number of Persons Killed in Collision w/ Rain'] = rain_death_persons_total
        stats['Rain']['Total Number of Persons Injured in Collision w/ Rain Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], ri_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], ri_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], ri_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Rain']['Total Number of Persons Killed in Collision w/ Rain Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], rd_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], rd_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], rd_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Rain']['Total Number of Collisions w/ Injury & Rain in Year'] = rain_injury_total
        stats['Rain']['Total Number of Collisions w/ Death & Rain in Year'] = rain_death_total
        stats['Rain']['Injury Rate w/ Rain (Year %)'] = float(rain_injury_total)/float(rain_total) * 100.00
        stats['Rain']['Death Rate w/ Rain (Year %)'] = float(rain_death_total)/float(rain_total) * 100.00
        stats['Rain']['Total Number of Rain Collisions in Year'] = rain_total
        stats['Rain']['Percent of Collisions in Rain Over Year (%)'] = (float(rain_total) / float(total)) * 100.00
        stats['Rain']['Collisions in Rain'] = [str(
                                            (months(x+1), r_arr[x][1])
                                            ) for x in range(0,12)]
        stats['Rain']['Average Collisions in Rain Per Month'] = rain_total//12
        stats['Rain']['Number of Collisions w/ Injuries and Rain Per Month'] = [str(
                                                                                (months(x+1), ri_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Rain']['Number of Collisions w/ Deaths and Rain Per Month'] = [str(
                                                                                (months(x+1), rd_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Rain']['Percent of Collisions in Each Month That Are Rain Collisions (Rain %)'] = [str(
                                                                (months(x+1), round((float(r_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Rain']['Injury Rate Per Month w/ Rain (Month %)'] = [str(
                                                                (months(x+1), round((float(ri_arr[x][1])/float(r_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if r_arr[x][1] != 0]
        stats['Rain']['Death Rate Per Month w/ Rain (Month %)'] = [str(
                                                                (months(x+1), round((float(rd_arr[x][1])/float(r_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if r_arr[x][1] != 0]




        # Snow Statistics
        stats['Snow'] = {}
        s_arr = [] # Snow Collision Data
        si_arr = [] # Snow Collision Injury Data
        sd_arr = [] # Snow Collision Death Data

        """PREPROCESSING"""
        # Snow Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Snow="1" and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            s_arr.append((x, sq.count()))
            sq.unpersist()
        snow_total = sum([x[1] for x in s_arr])

        # Snow Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Snow="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            si_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        snow_injury_total = sum([x[1] for x in si_arr])
        snow_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in si_arr])

        # Snow Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Snow="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            sd_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        snow_death_total = sum([x[1] for x in sd_arr])
        snow_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in sd_arr])




        # Statistics Deriving
        stats['Snow']['Total Number of Persons Injured in Collision w/ Snow'] = snow_injury_persons_total
        stats['Snow']['Total Number of Persons Killed in Collision w/ Snow'] = snow_death_persons_total
        stats['Snow']['Total Number of Persons Injured in Collision w/ Snow Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], si_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], si_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], si_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Snow']['Total Number of Persons Killed in Collision w/ Snow Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], sd_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], sd_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], sd_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Snow']['Total Number of Collisions w/ Injury & Snow in Year'] = snow_injury_total
        stats['Snow']['Total Number of Collisions w/ Death & Snow in Year'] = snow_death_total
        stats['Snow']['Injury Rate w/ Snow (Year %)'] = float(snow_injury_total)/float(snow_total) * 100.00
        stats['Snow']['Death Rate w/ Snow (Year %)'] = float(snow_death_total)/float(snow_total) * 100.00
        stats['Snow']['Total Number of Snow Collisions in Year'] = snow_total
        stats['Snow']['Percent of Collisions in Snow Over Year (%)'] = (float(snow_total) / float(total)) * 100.00
        stats['Snow']['Collisions in Snow'] = [str(
                                            (months(x+1), s_arr[x][1])
                                            ) for x in range(0,12)]
        stats['Snow']['Average Collisions in Snow Per Month'] = snow_total//12
        stats['Snow']['Number of Collisions w/ Injuries and Snow Per Month'] = [str(
                                                                                (months(x+1), si_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Snow']['Number of Collisions w/ Deaths and Snow Per Month'] = [str(
                                                                                (months(x+1), sd_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Snow']['Percent of Collisions in Each Month That Are Snow Collisions (Snow %)'] = [str(
                                                                (months(x+1), round((float(s_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Snow']['Injury Rate Per Month w/ Snow (Month %)'] = [str(
                                                                (months(x+1), round((float(si_arr[x][1])/float(s_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if s_arr[x][1] != 0]
        stats['Snow']['Death Rate Per Month w/ Snow (Month %)'] = [str(
                                                                (months(x+1), round((float(sd_arr[x][1])/float(s_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if s_arr[x][1] != 0]








        # Freezing Temperatures Statistics
        stats['Freezing'] = {}
        f_arr = [] # Freezing Collision Data
        fi_arr = [] # Freezing Collision Injury Data
        fd_arr = [] # Freezing Collision Death Data

        """PREPROCESSING"""
        # Freezing Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Freezing="1" and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            f_arr.append((x, sq.count()))
            sq.unpersist()
        freezing_total = sum([x[1] for x in f_arr])

        # Freezing Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Freezing="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            fi_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        freezing_injury_total = sum([x[1] for x in fi_arr])
        freezing_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in fi_arr])

        # Freezing Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Freezing="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            fd_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        freezing_death_total = sum([x[1] for x in fd_arr])
        freezing_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in fd_arr])




        # Statistics Deriving
        stats['Freezing']['Total Number of Persons Injured in Collision w/ Freezing'] = freezing_injury_persons_total
        stats['Freezing']['Total Number of Persons Killed in Collision w/ Freezing'] = freezing_death_persons_total
        stats['Freezing']['Total Number of Persons Injured in Collision w/ Freezing Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], fi_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], fi_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], fi_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Freezing']['Total Number of Persons Killed in Collision w/ Freezing Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], fd_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], fd_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], fd_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Freezing']['Total Number of Collisions w/ Injury & Freezing in Year'] = freezing_injury_total
        stats['Freezing']['Total Number of Collisions w/ Death & Freezing in Year'] = freezing_death_total
        stats['Freezing']['Injury Rate w/ Freezing (Year %)'] = float(freezing_injury_total)/float(freezing_total) * 100.00
        stats['Freezing']['Death Rate w/ Freezing (Year %)'] = float(freezing_death_total)/float(freezing_total) * 100.00
        stats['Freezing']['Total Number of Freezing Collisions in Year'] = freezing_total
        stats['Freezing']['Percent of Collisions in Freezing Over Year (%)'] = (float(freezing_total) / float(total)) * 100.00
        stats['Freezing']['Collisions in Freezing'] = [str(
                                            (months(x+1), f_arr[x][1])
                                            ) for x in range(0,12)]
        stats['Freezing']['Average Collisions in Freezing Per Month'] = freezing_total//12
        stats['Freezing']['Number of Collisions w/ Injuries and Freezing Per Month'] = [str(
                                                                                (months(x+1), fi_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Freezing']['Number of Collisions w/ Deaths and Freezing Per Month'] = [str(
                                                                                (months(x+1), fd_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Freezing']['Percent of Collisions in Each Month That Are Freezing Collisions (Freezing %)'] = [str(
                                                                (months(x+1), round((float(f_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Freezing']['Injury Rate Per Month w/ Freezing (Month %)'] = [str(
                                                                (months(x+1), round((float(fi_arr[x][1])/float(f_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if f_arr[x][1] != 0]
        stats['Freezing']['Death Rate Per Month w/ Freezing (Month %)'] = [str(
                                                                (months(x+1), round((float(fd_arr[x][1])/float(f_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if f_arr[x][1] != 0]





        # Low Visibility Statistics
        stats['LowVisibility'] = {}
        lv_arr = [] # Low Visibility Collision Data
        lvi_arr = [] # Low Visibility Collision Injury Data
        lvd_arr = [] # Low Visibility Collision Death Data

        """PREPROCESSING"""
        # Low Visibility Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where LowVisibility="1" and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            lv_arr.append((x, sq.count()))
            sq.unpersist()
        lowvisibility_total = sum([x[1] for x in lv_arr])

        # Low Visibility Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where LowVisibility="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            lvi_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        lowvisibility_injury_total = sum([x[1] for x in lvi_arr])
        lowvisibility_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in lvi_arr])

        # Low Visibility Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where LowVisibility="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            lvd_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        lowvisibility_death_total = sum([x[1] for x in lvd_arr])
        lowvisibility_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in lvd_arr])




        # Statistics Deriving
        stats['LowVisibility']['Total Number of Persons Injured in Collision w/ Low Visibility'] = lowvisibility_injury_persons_total
        stats['LowVisibility']['Total Number of Persons Killed in Collision w/ Low Visibility'] = lowvisibility_death_persons_total
        stats['LowVisibility']['Total Number of Persons Injured in Collision w/ Low Visibility Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], lvi_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], lvi_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], lvi_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['LowVisibility']['Total Number of Persons Killed in Collision w/ Low Visibility Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], lvd_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], lvd_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], lvd_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['LowVisibility']['Total Number of Collisions w/ Injury & Low Visibility in Year'] = lowvisibility_injury_total
        stats['LowVisibility']['Total Number of Collisions w/ Death & Low Visibility in Year'] = lowvisibility_death_total
        stats['LowVisibility']['Injury Rate w/ Low Visibility (Year %)'] = float(lowvisibility_injury_total)/float(lowvisibility_total) * 100.00
        stats['LowVisibility']['Death Rate w/ Low Visibility (Year %)'] = float(lowvisibility_death_total)/float(lowvisibility_total) * 100.00
        stats['LowVisibility']['Total Number of Low Visibility Collisions in Year'] = lowvisibility_total
        stats['LowVisibility']['Percent of Collisions in Low Visibility Over Year (%)'] = (float(lowvisibility_total) / float(total)) * 100.00
        stats['LowVisibility']['Collisions in Low Visibility'] = [str(
                                            (months(x+1), lv_arr[x][1])
                                            ) for x in range(0,12)]
        stats['LowVisibility']['Average Collisions in Low Visibility Per Month'] = lowvisibility_total//12
        stats['LowVisibility']['Number of Collisions w/ Injuries and Low Visibility Per Month'] = [str(
                                                                                (months(x+1), lvi_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['LowVisibility']['Number of Collisions w/ Deaths and Low Visibility Per Month'] = [str(
                                                                                (months(x+1), lvd_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['LowVisibility']['Percent of Collisions in Each Month That Are Low Visibility Collisions (LowVisibility %)'] = [str(
                                                                (months(x+1), round((float(lv_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['LowVisibility']['Injury Rate Per Month w/ Low Visibility (Month %)'] = [str(
                                                                (months(x+1), round((float(lvi_arr[x][1])/float(lv_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if lv_arr[x][1] != 0]
        stats['LowVisibility']['Death Rate Per Month w/ Low Visibility (Month %)'] = [str(
                                                                (months(x+1), round((float(lvd_arr[x][1])/float(lv_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if lv_arr[x][1] != 0]    





        # Windy Statistics
        stats['Windy'] = {}
        w_arr = [] # Windy Collision Data
        wi_arr = [] # Windy Collision Injury Data
        wd_arr = [] # Windy Collision Death Data

        """PREPROCESSING"""
        # Windy Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Windy="1" and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            w_arr.append((x, sq.count()))
            sq.unpersist()
        windy_total = sum([x[1] for x in w_arr])

        # Windy Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Windy="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            wi_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        windy_injury_total = sum([x[1] for x in wi_arr])
        windy_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in wi_arr])

        # Windy Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where Windy="1" and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            wd_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        windy_death_total = sum([x[1] for x in wd_arr])
        windy_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in wd_arr])




        # Statistics Deriving
        stats['Windy']['Total Number of Persons Injured in Collision w/ Windy'] = windy_injury_persons_total
        stats['Windy']['Total Number of Persons Killed in Collision w/ Windy'] = windy_death_persons_total
        stats['Windy']['Total Number of Persons Injured in Collision w/ Windy Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], wi_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], wi_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], wi_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Windy']['Total Number of Persons Killed in Collision w/ Windy Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], wd_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], wd_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], wd_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Windy']['Total Number of Collisions w/ Injury & Windy in Year'] = windy_injury_total
        stats['Windy']['Total Number of Collisions w/ Death & Windy in Year'] = windy_death_total
        stats['Windy']['Injury Rate w/ Windy (Year %)'] = float(windy_injury_total)/float(windy_total) * 100.00
        stats['Windy']['Death Rate w/ Windy (Year %)'] = float(windy_death_total)/float(windy_total) * 100.00
        stats['Windy']['Total Number of Windy Collisions in Year'] = windy_total
        stats['Windy']['Percent of Collisions in Windy Over Year (%)'] = (float(windy_total) / float(total)) * 100.00
        stats['Windy']['Collisions in Windy'] = [str(
                                            (months(x+1), w_arr[x][1])
                                            ) for x in range(0,12)]
        stats['Windy']['Average Collisions in Windy Per Month'] = windy_total//12
        stats['Windy']['Number of Collisions w/ Injuries and Windy Per Month'] = [str(
                                                                                (months(x+1), wi_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Windy']['Number of Collisions w/ Deaths and Windy Per Month'] = [str(
                                                                                (months(x+1), wd_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Windy']['Percent of Collisions in Each Month That Are Windy Collisions (Windy %)'] = [str(
                                                                (months(x+1), round((float(w_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Windy']['Injury Rate Per Month w/ Windy (Month %)'] = [str(
                                                                (months(x+1), round((float(wi_arr[x][1])/float(w_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if w_arr[x][1] != 0]
        stats['Windy']['Death Rate Per Month w/ Windy (Month %)'] = [str(
                                                                (months(x+1), round((float(wd_arr[x][1])/float(w_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if w_arr[x][1] != 0]



        # Inclement Weather Statistics
        stats['Inclement Weather'] = {}
        i_arr = [] # Inclement Weather Collision Data
        ii_arr = [] # Inclement Weather Collision Injury Data
        di_arr = [] # Inclement Weather Collision Death Data

        """PREPROCESSING"""
        # Inclement Weather Collision Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where (Rain="1" or Snow="1" or Freezing="1" or LowVisibility="1" or Windy="1") and DATETIME like "{0}"'.format(date)
            sq = sqlContext.sql(query)
            i_arr.append((x, sq.count()))
            sq.unpersist()
        inclement_total = sum([x[1] for x in i_arr])

        # Inclement Weather Collision Injury Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where (Rain="1" or Snow="1" or Freezing="1" or LowVisibility="1" or Windy="1") and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_INJURED!=0 or \
                                                                    NUMBER_OF_MOTORIST_INJURED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_INJURED!=0)'.format(date)
            df_injs = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_CYCLIST_INJURED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_MOTORIST_INJURED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_injs.groupBy("NUMBER_OF_PEDESTRIANS_INJURED").count().orderBy('count', ascending=False).collect())
            ii_arr.append((x, df_injs.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_injs.unpersist()
        inclement_injury_total = sum([x[1] for x in ii_arr])
        inclement_injury_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in ii_arr])

        # Inclement Weather Collision Death Data Query
        for x in range(1,13):
            date = "20{1}-0{0}%".format(x, yr) if x < 10 else "20{1}-{0}%".format(x, yr)
            query = 'select * from df where (Rain="1" or Snow="1" or Freezing="1" or LowVisibility="1" or Windy="1") and DATETIME like "{0}" and ( \
                                                                    NUMBER_OF_CYCLIST_KILLED!=0 or \
                                                                    NUMBER_OF_MOTORIST_KILLED!=0 or \
                                                                    NUMBER_OF_PEDESTRIANS_KILLED!=0)'.format(date)
            df_deths = sqlContext.sql(query)
            cyclist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_CYCLIST_KILLED").count().orderBy('count', ascending=False).collect())
            motorist = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_MOTORIST_KILLED").count().orderBy('count', ascending=False).collect())
            pedestrian = map(lambda x: (int(x[0]), x[1]), df_deths.groupBy("NUMBER_OF_PEDESTRIANS_KILLED").count().orderBy('count', ascending=False).collect())
            di_arr.append((x, df_deths.count(), {"Cyclist": cyclist, "Motorist": motorist, "Pedestrians": pedestrian}))
            df_deths.unpersist()

        inclement_death_total = sum([x[1] for x in di_arr])
        inclement_death_persons_total = sum([sum(map(lambda a: a[0]*a[1], x[2]['Cyclist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Motorist'])) + sum(map(lambda a: a[0]*a[1], x[2]['Pedestrians'])) for x in di_arr])




        # Statistics Deriving
        stats['Inclement Weather']['Total Number of Persons Injured in Collision w/ Inclement Weather'] = inclement_injury_persons_total
        stats['Inclement Weather']['Total Number of Persons Killed in Collision w/ Inclement Weather'] = inclement_death_persons_total
        stats['Inclement Weather']['Total Number of Persons Injured in Collision w/ Inclement Weather Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], ii_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], ii_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], ii_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Inclement Weather']['Total Number of Persons Killed in Collision w/ Inclement Weather Per Month'] = [str(
                                                                            (months(x+1), sum(map(lambda a: a[0]*a[1], di_arr[x][2]['Cyclist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], di_arr[x][2]['Motorist'])) + 
                                                                                          sum(map(lambda a: a[0]*a[1], di_arr[x][2]['Pedestrians'])))
                                                                            ) for x in range(0,12)]    
        stats['Inclement Weather']['Total Number of Collisions w/ Injury & Inclement Weather in Year'] = inclement_injury_total
        stats['Inclement Weather']['Total Number of Collisions w/ Death & Inclement Weather in Year'] = inclement_death_total
        stats['Inclement Weather']['Injury Rate w/ Inclement Weather (Year %)'] = float(inclement_injury_total)/float(inclement_total) * 100.00
        stats['Inclement Weather']['Death Rate w/ Inclement Weather (Year %)'] = float(inclement_death_total)/float(inclement_total) * 100.00
        stats['Inclement Weather']['Total Number of Inclement Weather Collisions in Year'] = inclement_total
        stats['Inclement Weather']['Percent of Collisions in Inclement Weather Over Year (%)'] = (float(inclement_total) / float(total)) * 100.00
        stats['Inclement Weather']['Collisions in Inclement Weather'] = [str(
                                            (months(x+1), i_arr[x][1])
                                            ) for x in range(0,12)]
        stats['Inclement Weather']['Average Collisions in Inclement Weather Per Month'] = inclement_total//12
        stats['Inclement Weather']['Number of Collisions w/ Injuries and Inclement Weather Per Month'] = [str(
                                                                                (months(x+1), ii_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Inclement Weather']['Number of Collisions w/ Deaths and Inclement Weather Per Month'] = [str(
                                                                                (months(x+1), di_arr[x][1])
                                                                                ) for x in range(0,12)]
        stats['Inclement Weather']['Percent of Collisions in Each Month That Are Inclement Weather Collisions (Inclement Weather %)'] = [str(
                                                                (months(x+1), round((float(i_arr[x][1])/float(m_arr[x][1]))*100.0, 2))
                                                                ) for x in range(0,12) if m_arr[x][1] != 0]
        stats['Inclement Weather']['Injury Rate Per Month w/ Inclement Weather (Month %)'] = [str(
                                                                (months(x+1), round((float(ii_arr[x][1])/float(i_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if i_arr[x][1] != 0]
        stats['Inclement Weather']['Death Rate Per Month w/ Inclement Weather (Month %)'] = [str(
                                                                (months(x+1), round((float(di_arr[x][1])/float(i_arr[x][1]) * 100.00),2))
                                                                ) for x in range(0,12) if i_arr[x][1] != 0]





        json_results = json.dumps(stats, indent=4, sort_keys=True)
        #print json_results
        path = "hdfs:///user/dzeng01/project/statistical_results_20{0}".format(yr)
        sc.parallelize([json_results]).coalesce(1).saveAsTextFile(path)


        # Finished with year processing. Clear and move on.
        sqlContext.dropTempTable('df')
        df.unpersist()
