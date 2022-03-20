from datetime import date
from pyspark.sql.window import Window
import pyspark
from pyspark.sql import SparkSession;
import pyspark.sql.functions as F;
from pyspark.sql.types import *
import pandas as pd; 
import numpy as np; 
import pyspark.pandas as ps
import zipfile; 
import os; 


spark = SparkSession.builder.appName('Exercise6') \
            .enableHiveSupport().getOrCreate()
# method to walk the data directory and return the path endpoint to each zip file; 
def walkDataDir(presentDirectory):

    zippedFiles = []; 

    for root, dir_names ,fileNames in os.walk(presentDirectory):

        for f in fileNames: 

            zippedFiles.append(os.path.join(root,f))

    return zippedFiles; 


def readZipContents(zipIndex,csvIndex):

        files = walkDataDir(os.path.abspath('./data'))

        zip_file = zipfile.ZipFile(files[zipIndex])

        name_list = zip_file.namelist()

        dflist = [];

        with zipfile.ZipFile(files[zipIndex], 'r') as zip:

            with zip.open(zip.namelist()[csvIndex]) as myFile: 

                dflist.append(pd.read_csv(myFile))

        df = pd.concat(dflist)
        
        return df.values.tolist(), df.columns.tolist(),df
    

def averageTripsAndTotalTrips(dataFrame):

    # Average Trip duration and total trips taken per day; 
    dataFrame = dataFrame[['start_time','trip_id','tripduration'

        ]].assign(startTimeCol= lambda x: (pd.to_datetime(x['start_time']).dt.strftime('%m/%d/%Y'))).groupby('start_time').agg(

            daily_trip_count=('trip_id','count'),

                average_trip_duration=('trip_id', 'mean')

                ).sort_index();

    dataFrame['average_trip_duration'] = dataFrame['average_trip_duration'].round(2)

    return dataFrame.values.tolist(), dataFrame.columns.tolist()

    
def stationPopularityByMonth(dataFrame):
    # formatting the start and end time columns as mm/dd/yyyy format; 
    dataFrame = dataFrame[['start_time','from_station_name','tripduration']].assign(

        month=lambda day: (

            ps.to_datetime(day.start_time).dt.month_name())

                ).groupby(['month','from_station_name'],

                    as_index=False).count().rename(columns={

                        'tripduration':'total_trip_count'

                            }).sort_values(['month','total_trip_count'], 

                                ascending=False).groupby('month', 

                                    as_index=False).first();

    return dataFrame


def stationPopularityByDay(dataFrame):
    # What were the top 3 trip stations each day for the last two weeks?
    # formatting the date columns and formatting the day day name; 
    dailyPopularity = dataFrame.copy().assign(

        start_time=lambda date: (ps.to_datetime(date['start_time']).dt.strftime('%m/%d/%Y'))).assign(

            end_time=lambda date: (ps.to_datetime(date['end_time']).dt.strftime('%m/%d/%Y'))).assign(

                day_of_week=lambda day: (ps.to_datetime(dataFrame['start_time']).dt.day_name())).loc[dataFrame.start_time >=  ps.to_datetime(

                  ps.to_datetime(dataFrame['start_time']).max() - pd.Timedelta("14 days"))][['start_time',

                        'day_of_week','from_station_name','trip_id']].groupby(['start_time',

                        'from_station_name','day_of_week'], as_index=False).count().rename(

                            columns={'trip_id':'total_trip_count'}).sort_values(       

                                ['start_time','total_trip_count']).groupby(

                                    'start_time').tail(3);

    return dailyPopularity  


def averageTripDurationByGender(dataFrame):
    # Do Males or Females take longer trips on average?

    # # dataframe for trips length; 
    genderStats = dataFrame[['gender',

    'tripduration']].groupby('gender', as_index=False).mean().assign(

        tripduration=lambda x: x['tripduration'].round(2)

            ).applymap(str).assign(

                    avg_trip_in_minutes= lambda newForm: (

                            newForm.tripduration.str.replace(r'.',':', regex=True)

                                )).drop(columns=['tripduration']);

    return genderStats;

def longestShortestTripByAge(dataFrame):

    # What is the top 10 ages of those that take the longest trips, and shortest?
    tripByAge = dataFrame.copy().loc[((dataFrame['gender'] != '') & (dataFrame['birthyear'] != ''))].assign(

        current_year=date.today().year

            ).assign(birthyear=lambda convertFloat: convertFloat['birthyear'].round(0)).assign(

                age=lambda age: (age['current_year'] - age['birthyear'])

                    )[['age','birthyear','tripduration']].groupby(['age','birthyear'], as_index=False).mean().sort_values(

                        ['tripduration','age'],ascending=False).assign(

                                avg_tripduration_by_age=lambda minutes: (minutes['tripduration']).round(2)
                                
                                ).drop(columns=['tripduration']);

    # shortest and longest trips in separate data frames; 
    longestTrips = tripByAge.copy().head(10).sort_values(

        'avg_tripduration_by_age', ascending=False).applymap(str).assign(

            avg_tripduration_by_age= lambda newForm: (newForm['avg_tripduration_by_age'].str.replace(r'.',':', regex=True))

            );

    shortestTrips = tripByAge.copy().tail(10).sort_values(

        'avg_tripduration_by_age').applymap(str).assign(

            avg_tripduration_by_age= lambda newForm: (newForm['avg_tripduration_by_age'].str.replace(r'.',':', regex=True))

            );

    return longestTrips, shortestTrips;

def main():    

    df, columns, actualDf = readZipContents(1,0)

    values, columns = averageTripsAndTotalTrips(actualDf); 

    # # # Quarter 19 contents; 
    sc = spark.sparkContext; 

    df = spark.createDataFrame(sc.parallelize(values),columns)

    df.printSchema()

    df.show()


    # df = df.collect(); 

    # print(df)

    # # most popular trips for each month; 
    # monthDf19 = stationPopularityByMonth(quarter19);

    # # popularity for the past two weeks; 
    # weekly19 = stationPopularityByDay(quarter19);

    # # mean tripduration by gender; 
    # genderStats = averageTripDurationByGender(quarter19); 

    # # longest and shortest trips by age group; 
    # longestTrips, shortestTrips = longestShortestTripByAge(quarter19); 

    # print(longestTrips)

    # spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    # dir1/file3.json is corrupt from parquet's view
    # test_corrupt_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.zip").load(os.path.abspath('./data'));

    # print(test_corrupt_df.show())


if __name__ == '__main__':
    main()