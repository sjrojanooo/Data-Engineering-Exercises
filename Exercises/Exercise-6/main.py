import pyspark;
from pyspark.sql import SparkSession;
from pyspark.sql.window import Window;
from pyspark.sql import functions as F;
from pyspark.sql import types as T;
import pandas as pd; 
import zipfile; 
import os; 

# creates the sparksession instance; 
spark = SparkSession.builder.appName("Exercise6") \
            .enableHiveSupport().getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("parquet.enable.summary-metadata", "false")

# creates instance of sparkContext; 
sc = spark.sparkContext; 

# creates a pyspark data frame; 
def create_frame(dataFrame):

    # partition number used to split the large list from the csv file 
    # into multiple small ones to reduce the message size; 
    partitionNumber = 100; 

    return spark.createDataFrame(sc.parallelize(dataFrame.values.tolist(),partitionNumber),dataFrame.columns.tolist())

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
    
# discoverd average trip duration and trips taken each day; 
def averageTripsAndTotalTrips(dataFrame):

    # 1. formats date time to mm/dd/yyyy format; 
    # 2. performs a group by on the start_time key, I removed the timestamp from them so that days would have the same format; 
    # 3. performs a count on the trip_id, rounds to two decimal places, and creates an alias or new new name for the tripduration column 
    # 4. Ordering by the 

    dataFrame = dataFrame.withColumnRenamed("start_time","date")\
            .withColumn("date",F.to_timestamp("date").cast("date"))\
                .groupBy("date")\
                    .agg(F.count(dataFrame.trip_id).alias("total_trips"),
                        F.round(F.mean(dataFrame.tripduration),2).alias("avg_trip_time"))\
                            .orderBy("date")
    return dataFrame

# returns most popular station by month; 
def stationPopularityByMonth(dataFrame):
    # will be used to partition the result of the groupby statement. 
    w = Window().partitionBy("month").orderBy(F.desc("total_trips"))

    dataFrame = dataFrame.select("start_time","from_station_name","trip_id")\
            .withColumn("start_time", F.to_timestamp("start_time").cast("date"))\
                .withColumn("month", F.date_format("start_time","MMMM"))\
                    .groupBy("from_station_name","month").agg(F.count("trip_id").alias("total_trips"))\
                        .withColumn("rank",F.dense_rank().over(w)).where(F.col("rank") == 1)

    return dataFrame

# What were the top 3 trip stations each day for the last two weeks?
def stationPopularityByDay(dataFrame):

    # creates a dummy column and assigns a literal/constant value to it; 
    # I am using it to filter over the 14 day benchmark; 
    w = Window().orderBy(F.lit(1)); 
    # window to partition total trips by start_time 
    # I am using dense_rank because rank will skip positios after any equal rankings; 
    w2 = Window().partitionBy("start_time").orderBy(F.desc("total_trips"))

    dataFrame = dataFrame.select("start_time","from_station_name","trip_id")\
                    .withColumn("start_time", F.to_timestamp("start_time").cast("date"))\
                        .withColumn("day_of_week", F.date_format("start_time","E"))\
                            .groupBy("start_time","day_of_week","from_station_name")\
                                .agg(F.count("trip_id").alias("total_trips"))\
                                    .withColumn("max_date", F.max("start_time").over(w))\
                                        .filter("start_time >= max_date - interval 14 days").drop("max_date")\
                                            .withColumn("rank", F.dense_rank().over(w2))\
                                                .where((F.col("rank") <= 3))\
                                                    .orderBy("start_time","rank")

    return dataFrame

# Do Males or Females take longer trips on average?
def averageTripDurationByGender(dataFrame):
    # Females take the longest trips. 

    w = Window().partitionBy("gender").orderBy("tripduration")

    dataFrame = dataFrame.select("gender","tripduration")\
            .groupBy("gender")\
                .agg(F.round(F.mean("tripduration"),2).alias("avg_trip"))\
                        .replace("NaN", None).dropna()
    return dataFrame

# What are the top 10 ages of those that take the longest trips, and shortest?            
def longestShortestTripByAge(dataFrame):

    dataFrame = dataFrame.select("birthyear","tripduration")\
                    .replace("NaN", None).dropna()\
                        .withColumn("birthyear", dataFrame["birthyear"].cast(T.IntegerType()))\
                            .withColumn("current_year", F.year(F.current_timestamp()).cast(T.IntegerType()))\
                                .withColumn("age_group", F.col("current_year")-F.col("birthyear"))\
                                    .groupby("age_group").agg(F.round(F.mean("tripduration"),2).alias("avg_trip_time"))\
                                        .sort(F.desc("avg_trip_time"))\
                                            .withColumn("rank", F.monotonically_increasing_id()+1)

    # Top 10 longest trips by age; 
    longest = dataFrame.where(F.col("rank") <= 10)
    # Top 10 shortest trips by age; 
    shortest = dataFrame.filter(F.col("rank") >= dataFrame.count() - 10)


    return  longest, shortest

def main():    

    # Quarter 19 contents; 
    quarter19DataFrame = create_frame(readZipContents(1,0))

    # What is the average trip duration per day?
    # How many trips were taken each day?

    # averageTotalTripsDf = averageTripsAndTotalTrips(quarter19DataFrame)

    # averageTotalTripsDf.show(50)

    # monthDf19 = stationPopularityByMonth(quarter19DataFrame).show(50); 

    # dailyPopularity19 = stationPopularityByDay(quarter19DataFrame).show(50); 

    # tripDurationByGender = averageTripDurationByGender(quarter19DataFrame).show(50); 

    longestTrips, shortestTrips= longestShortestTripByAge(quarter19DataFrame)

    longestTrips.show()

    # longestTrips

    # longestTripsByGender, shortestTripsByGender = longestShortestTripByAge(quarter19DataFrame); 


    #ENDS QUARTER 19 DIVVY TRIP INFO; 

if __name__ == "__main__":
    main()