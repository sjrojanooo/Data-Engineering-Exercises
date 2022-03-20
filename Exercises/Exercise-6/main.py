from datetime import date
import pdb
from unicodedata import decimal
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

sc = spark.sparkContext; 

def create_frame(dataFrame):

    return spark.createDataFrame(sc.parallelize(dataFrame.values.tolist()),dataFrame.columns.tolist())

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

        datalist = [];

        with zipfile.ZipFile(files[zipIndex], 'r') as zip:

            with zip.open(zip.namelist()[csvIndex]) as myFile: 

                datalist.append(pd.read_csv(myFile,
                    thousands=',',
                    decimal='.',
                ))

        data_frame = pd.concat(datalist)
        
        return data_frame
    

def averageTripsAndTotalTrips(dataFrame):

    # Average Trip duration and total trips taken per day; 
    dataFrame = dataFrame[['start_time','trip_id','tripduration'

        ]].assign(start_time= lambda x: (pd.to_datetime(x['start_time']).dt.strftime('%m/%d/%Y'))).groupby('start_time',as_index=False).agg(

            daily_trip_count=('trip_id','count'),

                average_trip_duration=('tripduration','mean')

                ).assign(

                    average_trip_duration=lambda x: (round(x['average_trip_duration'] / 60,2))
                    
                        ).assign(

                            average_trip_duration=lambda x: x['average_trip_duration'].astype(str).str.replace(r'.',':',regex=True)
                            
                            )

    return  create_frame(dataFrame)
    
def stationPopularityByMonth(dataFrame):

    # formatting the start and end time columns as mm/dd/yyyy format; 
    dataFrame = dataFrame[['start_time','from_station_name','tripduration']].assign(

        month=lambda day: (pd.to_datetime(
            
                day.start_time).dt.month_name())

                ).groupby(['month','from_station_name'],as_index=False).count().rename(
                    
                    columns={'tripduration':'total_trip_count'

                            }).sort_values('total_trip_count').drop(

                                columns=['start_time']).groupby('month', 

                                    as_index=False).first();

    return create_frame(dataFrame)


def stationPopularityByDay(dataFrame):

    # What were the top 3 trip stations each day for the last two weeks?
    # formatting the date columns and formatting the day day name; 
    dailyPopularity = dataFrame.copy().assign(

                day_of_week=lambda day: (pd.to_datetime(dataFrame['start_time']).dt.day_name())).loc[

                    pd.to_datetime(dataFrame['start_time']) >= pd.to_datetime(dataFrame['start_time'].max()) - pd.Timedelta(days=14)

                        ].assign(

                            start_time=lambda x: (pd.to_datetime(x['start_time']).dt.strftime('%m/%d/%Y')))[
                                
                                ['start_time','day_of_week','from_station_name','trip_id']].groupby([

                                            'start_time','day_of_week','from_station_name'
                                            
                                                ], as_index=False).agg(

                                                    total_trip_count=('trip_id','count')

                                                        ).sort_values(
                                                            
                                                            ['start_time','total_trip_count']).groupby('start_time').tail(3)


    return create_frame(dailyPopularity)


def averageTripDurationByGender(dataFrame):

    # Do Males or Females take longer trips on average?

    # # dataframe for trips length; 
    genderStats = dataFrame[['gender',

    'tripduration']].groupby('gender', as_index=False).mean().assign(

        tripduration=lambda x: round(x['tripduration']/60,2)

            ).applymap(str).assign(

                    avg_trip_in_minutes= lambda newForm: (

                            newForm['tripduration'].str.replace(r'.',':', regex=True)

                                )).drop(columns=['tripduration']);

    return create_frame(genderStats);


def longestShortestTripByAge(dataFrame):

    # What is the top 10 ages of those that take the longest trips, and shortest?
    tripByAge = dataFrame.copy().loc[((dataFrame['gender'] != '') & (dataFrame['birthyear'] != ''))].assign(

        current_year=date.today().year

            ).assign(birthyear=lambda convertFloat: convertFloat['birthyear'].round(0)).assign(

                age=lambda age: (age['current_year'] - age['birthyear'])

                    )[['age','birthyear','tripduration']].groupby(['age','birthyear'], as_index=False).mean().sort_values(

                        ['tripduration','age'],ascending=False).assign(

                                avg_minutes=lambda minutes: (minutes['tripduration']/60).round(2)
                                
                                ).drop(columns=['tripduration']);


    # shortest and longest trips in separate data frames; 
    longestTrips = tripByAge.copy().head(10).sort_values(

        'avg_minutes', ascending=False).applymap(str).assign(

            avg_minutes= lambda newForm: (newForm['avg_minutes'].str.replace(r'.',':', regex=True))

            );

    shortestTrips = tripByAge.copy().tail(10).sort_values(

        'avg_minutes').applymap(str).assign(

            avg_minutes= lambda newForm: (newForm['avg_minutes'].str.replace(r'.',':', regex=True))

            );

    return create_frame(longestTrips), create_frame(shortestTrips);

def main():    

    # Quarter 19 contents; 
    quarter19DataFrame = readZipContents(1,0)

    averageTotalTripsDf = averageTripsAndTotalTrips(quarter19DataFrame); 

    # most popular trips for each month; 
    monthDf19 = stationPopularityByMonth(quarter19DataFrame);

    monthlyPopularity19 = stationPopularityByMonth(quarter19DataFrame); 

    dailyPopularity19 = stationPopularityByDay(quarter19DataFrame); 

    tripDurationByGender = averageTripDurationByGender(quarter19DataFrame); 

    longestTripsByGender, shortestTripsByGender = longestShortestTripByAge(quarter19DataFrame); 

    longestTripsByGender.show()

    shortestTripsByGender.show()

    #ENDS QUARTER 19 DIVVY TRIP INFO; 


if __name__ == '__main__':
    main()