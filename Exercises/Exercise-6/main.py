from pyspark.sql import SparkSession; # to initialize spark session; 
from pyspark.sql.window import Window; # to use pyspark sql window functionalities; 
from pyspark.sql import functions as F; # importing sparks built in functions; 
from pyspark.sql import types as T; # importing types to handles data types; 
import pandas as pd; # using pandas to concatentate csv 
import zipfile; # for reading zipfile contents
import os; # for operating system functionalities; 

# creates the sparksession instance; 
spark = SparkSession.builder.appName("Exercise6") \
            .enableHiveSupport().getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("parquet.enable.summary-metadata", "false")

# creates instance of sparkContext; 
sc = spark.sparkContext; 

# BEGINS EXTRACT LEVEL; 
# method to walk the data directory and return the path endpoint to each zip file; 
def walkDataDir(presentDirectory):

    zippedFiles = []; 

    for root, dir_names ,fileNames in os.walk(presentDirectory):

        for f in fileNames: 

            zippedFiles.append(os.path.join(root,f))

    return zippedFiles; 

# read all csv files and returning the dataframe; 
def readZipContents(zipIndex,csvIndex):

        files = walkDataDir(os.path.abspath("./data"))

        datalist = [];

        with zipfile.ZipFile(files[zipIndex], "r") as zip:

            with zip.open(zip.namelist()[csvIndex]) as myFile: 

                datalist.append(pd.read_csv(myFile,
                    thousands=",",
                    decimal=".",
                    parse_dates=True
                ))

        data_frame = pd.concat(datalist)
        
        return data_frame

# ENDS EXTRACT PHASE; 

# BEGINS TRANSFORM PHASE; 
# creates a pyspark data frame; 
def create_frame(dataFrame):

    # partition number used to split the large list from the csv file 
    # into multiple small ones to reduce the message size; 
    partitionNumber = 100; 

    return spark.createDataFrame(sc.parallelize(dataFrame.values.tolist(),partitionNumber),dataFrame.columns.tolist())

# discoverd average trip duration and trips taken each day; 
def averageTripsAndTotalTrips(dataFrame, startTimeCol, endTimeCol, tripIDCol):

    # 1. formats date time to mm/dd/yyyy format; 
    # 2. performs a group by on the start_time key, I removed the timestamp from them so that days would have the same format; 
    # 3. performs a count on the trip_id, rounds to two decimal places, and creates an alias or new new name for the tripduration column 
    dataFrame = dataFrame.select(startTimeCol,endTimeCol, tripIDCol)\
                    .withColumn("date",F.date_format(F.to_timestamp(startTimeCol),"MM-dd-yyyy"))\
                        .withColumn(startTimeCol, F.to_timestamp(startTimeCol).cast(T.LongType()))\
                            .withColumn(endTimeCol, F.to_timestamp(endTimeCol).cast(T.LongType()))\
                                .withColumn("trip_in_minutes", F.round((F.col(endTimeCol) - F.col(startTimeCol))/60,2))\
                                    .select("date","trip_in_minutes",tripIDCol)\
                                        .groupBy("date")\
                                            .agg(F.count(F.col(tripIDCol)).alias("total_trips"),
                                                F.round(F.mean(F.col("trip_in_minutes")),2).alias("avg_trip_time_in_minutes"))\
                                                    .orderBy("date")                    
    return dataFrame

# returns most popular station by month; 
def stationPopularityByMonth(dataFrame, dateCol, stationCol, tripIDCol):

    # will be used to partition the result of the groupby statement. 
    # ranks all stations by the total trip count with each corresponding month;
    w = Window().partitionBy("month").orderBy(F.desc("total_trips"))

    dataFrame = dataFrame.select(dateCol,stationCol,tripIDCol)\
            .withColumn(dateCol, F.to_timestamp(dateCol).cast("date"))\
                .withColumn("month", F.date_format(dateCol,"MMMM"))\
                    .groupBy(stationCol,"month").agg(F.count(tripIDCol).alias("total_trips"))\
                        .withColumn("rank",F.dense_rank().over(w)).where(F.col("rank") == 1)

    return dataFrame

# What were the top 3 trip stations each day for the last two weeks?
def stationPopularityByDay(dataFrame,dateCol, stationCol, tripIdCol):

    # window to partition total trips by start_time 
    # I am using dense_rank because rank will skip positios after any equal rankings; 
    w = Window().partitionBy(dateCol).orderBy(F.desc("total_trips"))

    # capturing the max date and using the date_sub function to return the date 14 days prior; 
    max_date = dataFrame.select(F.date_sub(F.to_date(F.max(dateCol)),14)\
                    .alias("max_date"))\
                        .collect()[0]["max_date"]

    dataFrame = dataFrame.select(dateCol,stationCol,tripIdCol)\
                    .withColumn(dateCol, F.to_date(dateCol))\
                        .withColumn("day_of_week", F.date_format(dateCol,"E"))\
                            .groupBy(dateCol,"day_of_week",stationCol)\
                                .agg(F.count(tripIdCol).alias("total_trips"))\
                                    .where(F.col(dateCol) >= max_date)\
                                        .withColumn("rank", F.dense_rank().over(w))\
                                            .where((F.col("rank") <=3))\
                                                .orderBy(dateCol, "rank")

    return dataFrame

# Do Males or Females take longer trips on average?
def averageTripDurationByGender(dataFrame):

    w = Window().partitionBy("gender").orderBy("tripduration")

    dataFrame = dataFrame.select("gender","tripduration")\
                    .groupBy("gender")\
                        .agg(F.round(F.mean("tripduration") / 60,2)\
                            .alias("avg_trip_time_in_minutes"))\
                                .replace("NaN", None).dropna()
    return dataFrame

# What are the top 10 ages of those that take the longest trips, and shortest?            
def longestShortestTripByAge(dataFrame):

    dataFrame = dataFrame.select("birthyear","tripduration")\
                    .replace("NaN", None).dropna()\
                        .withColumn("birthyear", dataFrame["birthyear"].cast(T.IntegerType()))\
                            .withColumn("current_year", F.year(F.current_date()).cast(T.IntegerType()))\
                                .withColumn("age_group", F.col("current_year")-F.col("birthyear"))\
                                    .groupby("age_group")\
                                        .agg(F.round(F.mean("tripduration") / 60,2).alias("avg_trip_time_in_minutes"))\
                                            .sort(F.desc("avg_trip_time_in_minutes"))\
                                                .withColumn("rank", F.monotonically_increasing_id()+1)

    # Top 10 longest trips by age; 
    longest = dataFrame\
                .where(F.col("rank") <= 10)


    # Top 10 shortest trips by age; 
    shortest = dataFrame\
                .where(F.col("rank") > dataFrame.count() - 10)\
                    .sort(F.desc("rank"))\
                        .withColumn("rank", F.monotonically_increasing_id()+1)

                

    return  longest, shortest

# ENDS TRANSFORM PHASE; 

# BEGINS LOAD PHASE
# Method to write out each dataframe to a csv files, parameters are the year and report name; 
# since pyspark writes out dataframes in parts, I am using coalesce to make sure that the data is written out 
# in only one part; 
# Note: the option below will overwrite any report directories are that are there; 
def writeToCSV(dataFrame,reportYear,reportName):

    dataFrame.coalesce(1).write.format("csv").mode("overwrite")\
        .option("header","true")\
        .save("reports/"+reportYear+"/"+reportName)

# ENDS LOAD PHASE

def main():    

    # # Quarter 19 contents; 
    quarter19DataFrame = create_frame(readZipContents(1,0));
    
    writeToCSV(averageTripsAndTotalTrips(quarter19DataFrame, "start_time","end_time","trip_id"),"2019","Avg-Trip-length-And-Total-Trips");

    writeToCSV(stationPopularityByMonth(quarter19DataFrame,"start_time", "from_station_name","trip_id"), "2019","Monthly-Popularity");

    writeToCSV(stationPopularityByDay(quarter19DataFrame,"start_time", "from_station_name", "trip_id"),"2019", "Daily-Popularity");

    writeToCSV(averageTripDurationByGender(quarter19DataFrame),"2019", "Avg-Trip-By-Gender");

    writeToCSV(longestShortestTripByAge(quarter19DataFrame)[0], "2019", "Longest-Trips-By-Age")

    writeToCSV(longestShortestTripByAge(quarter19DataFrame)[1], "2019", "Shortest-Trips-By-Age")

    # #ENDS QUARTER 19 DIVVY TRIP INFO; 

    # # Quarter 20 contents; 
    quarter20DataFrame = create_frame(readZipContents(0,0))

    writeToCSV(averageTripsAndTotalTrips(quarter20DataFrame, "started_at","ended_at","ride_id"),"2020","Avg-Trip-length-And-Total-Trips");

    writeToCSV(stationPopularityByMonth(quarter20DataFrame, "started_at", "start_station_name", "ride_id"), "2020","Monthly-Popularity");

    writeToCSV(stationPopularityByDay(quarter20DataFrame, "started_at", "start_station_name", "ride_id"),"2020", "Daily-Popularity");
    #ENDS QUARTER 20 DIVVY TRIP INFO; 


if __name__ == "__main__":
    main()