import pyspark
from pyspark.sql import SparkSession;
from pyspark.sql.window import Window;
from pyspark.sql import functions as F
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

    # formats date time to mm/dd/yyyy format; 
    return dataFrame.withColumn("start_time",F.to_timestamp("start_time").cast("date"))\
            .groupBy("start_time")\
                .agg(F.count(dataFrame.trip_id).alias("total_trips"),
                    F.round(F.mean(dataFrame.tripduration)/2 ,2).alias("avg_trip_time"))\
                        .orderBy("start_time")

# returns most popular station by month; 
def stationPopularityByMonth(dataFrame):
    # will be used to partition the result of the groupby statement. 
    w = Window().partitionBy("month").orderBy(F.desc("total_trips"))

    return dataFrame.select("start_time","from_station_name","trip_id")\
            .withColumn("start_time", F.to_timestamp("start_time").cast("date"))\
                .withColumn("month", F.date_format("start_time","MMMM"))\
                    .groupBy("from_station_name","month").agg(F.count("trip_id").alias("total_trips"))\
                        .withColumn("rank",F.dense_rank().over(w)).where(F.col("rank") == 1)

# What were the top 3 trip stations each day for the last two weeks?
def stationPopularityByDay(dataFrame):
    
    w = Window().orderBy(F.lit(1)); 
    # window to partition total trips by start_time 
    # I am using dense_rank because rank will skip positios after any equal rankings; 
    w2 = Window().partitionBy("start_time").orderBy(F.desc("total_trips"))

    return dataFrame.select("start_time","from_station_name","trip_id")\
                    .withColumn("start_time", F.to_timestamp("start_time").cast("date"))\
                        .withColumn("day_of_week", F.date_format("start_time","E"))\
                            .groupBy("start_time","day_of_week","from_station_name")\
                                .agg(F.count("trip_id").alias("total_trips"))\
                                    .withColumn("max_date", F.max("start_time").over(w))\
                                        .filter("start_time >= max_date - interval 14 days").drop("max_date")\
                                            .withColumn("rank", F.dense_rank().over(w2))\
                                                .where((F.col("rank") ==1)| (F.col("rank") ==2)| (F.col("rank") ==3))\
                                                    .orderBy("start_time","rank")

# Do Males or Females take longer trips on average?
def averageTripDurationByGender(dataFrame):
    
    return dataFrame.select("gender","tripduration")\
        .groupBy("gender")\
            .agg(F.round(F.mean("tripduration"),2).alias("avg_trip"))\
                    .replace("NaN", None).dropna()
                
# def longestShortestTripByAge(dataFrame):

    # # What is the top 10 ages of those that take the longest trips, and shortest?
    # tripByAge = dataFrame.copy().loc[((dataFrame["gender"] != "") & (dataFrame["birthyear"] != ""))].assign(

    #     current_year=date.today().year

    #         ).assign(birthyear=lambda convertFloat: convertFloat["birthyear"].round(0)).assign(

    #             age=lambda age: (age["current_year"] - age["birthyear"])

    #                 )[["age","birthyear","tripduration"]].groupby(["age","birthyear"], as_index=False).mean().sort_values(

    #                     ["tripduration","age"],ascending=False).assign(

    #                             avg_minutes=lambda minutes: (minutes["tripduration"]/60).round(2)
                                
    #                             ).drop(columns=["tripduration"]);


    # # shortest and longest trips in separate data frames; 
    # longestTrips = tripByAge.copy().head(10).sort_values(

    #     "avg_minutes", ascending=False).applymap(str).assign(

    #         avg_minutes= lambda newForm: (newForm["avg_minutes"].str.replace(r".",":", regex=True))

    #         );

    # shortestTrips = tripByAge.copy().tail(10).sort_values(

    #     "avg_minutes").applymap(str).assign(

    #         avg_minutes= lambda newForm: (newForm["avg_minutes"].str.replace(r".",":", regex=True))

    #         );

    # return create_frame(longestTrips), create_frame(shortestTrips);

def main():    

    # Quarter 19 contents; 
    quarter19DataFrame = create_frame(readZipContents(1,0))

    # What is the average trip duration per day?
    # How many trips were taken each day?
    
    # averageTotalTripsDf = averageTripsAndTotalTrips(quarter19DataFrame)

    # monthDf19 = stationPopularityByMonth(quarter19DataFrame); 

    # dailyPopularity19 = stationPopularityByDay(quarter19DataFrame); 

    # tripDurationByGender = averageTripDurationByGender(quarter19DataFrame); 

    # longestTripsByGender, shortestTripsByGender = longestShortestTripByAge(quarter19DataFrame); 


    #ENDS QUARTER 19 DIVVY TRIP INFO; 

    # QUARTER 20 contents; 

if __name__ == "__main__":
    main()