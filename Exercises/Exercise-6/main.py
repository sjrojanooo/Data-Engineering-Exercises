from datetime import datetime
from pyspark.sql import SparkSession;
import pandas as pd; 
import numpy as np; 
import zipfile; 
import os; 


# method to walk the data directory and return the path endpoint to each zip file; 
def walkDataDir(presentDirectory):

    dataPath = f'{presentDirectory}/data'; 

    zippedFiles = []; 

    for root, dir_names ,fileNames in os.walk(dataPath):

        for f in fileNames: 

            zippedFiles.append(os.path.join(root,f))

    return zippedFiles; 


def readZipContents():

    pass


def averageTripsAndTotalTrips():

    pass

def stationPopularityByMonth():

    pass 


def stationPopularityByDay():

    pass


def averageTripDurationByGender():

    pass 

def tripDurationByAge(df, genderCol, birthCol, tripColumn):

    pass

def main():

    # spark = SparkSession.builder.appName('Exercise6') \
    #     .enableHiveSupport().getOrCreate()

    # print(spark)

    zipResponse = walkDataDir(os.path.abspath(''))    

    zip_file = zipfile.ZipFile(zipResponse[1])

    q19 = pd.read_csv(zip_file.open(zip_file.namelist()[0]), 
    parse_dates=True,
    thousands=',',
    decimal='.',
    dtype={'trip_id':str}
    ).fillna('')

    q19 = q19.sort_values(['start_time', 'tripduration'], ascending=True); 

    # getting the data types that are in the csv file; 
    print(q19.dtypes)

    # printing out columns names; 
    print(q19.columns)

    # making copy of initial data frame; 
    averageTripsTotalTrips = q19.copy(); 

    # Average Trip duration and total trips taken per day; 
    averageTripsTotalTrips = averageTripsTotalTrips[['start_time','trip_id','tripduration'
    ,'from_station_id','from_station_name']].groupby(['start_time'], as_index=False).agg(
        daily_trip_count=('trip_id','count'),
        average_trip_duration=('tripduration', 'mean')
    );

    # most popular trips for each month; 
    stationPopularity = q19.copy()

    # formatting the start and end time columns as mm/dd/yyyy format; 
    stationPopularity = q19.copy().assign(
        start_time=lambda date: (pd.to_datetime(date['start_time']).dt.strftime('%m/%d/%Y'))).assign(
            end_time=lambda date: (pd.to_datetime(date['end_time']).dt.strftime('%m/%d/%Y'))).assign(
                month=lambda day: (pd.to_datetime(day['start_time']).dt.month_name()))


    popularityByMonth = stationPopularity.copy()

    # What was the most popular starting trip station for each month?
    popularityByMonth = popularityByMonth[['month',
        'from_station_name','trip_id']].groupby(['month','from_station_name'],
        as_index=False).agg(
        most_popular_month=('trip_id','count')
    ).sort_values(['month','most_popular_month'], 
    ascending=False).groupby('month', 
    as_index=False).first()

    # What were the top 3 trip stations each day for the last two weeks?
    # formatting the date columns and formatting the day day name; 
    dailyPopularity = stationPopularity.copy().assign(
        start_time=lambda date: (pd.to_datetime(date['start_time']).dt.strftime('%m/%d/%Y'))).assign(
            end_time=lambda date: (pd.to_datetime(date['end_time']).dt.strftime('%m/%d/%Y'))).assign(
                day_of_week=lambda day: (pd.to_datetime(day['start_time']).dt.day_name()))
        
    # formatting the start and end time columns as mm/dd/yyyy format; 
    maxDate= pd.to_datetime(pd.Timestamp(dailyPopularity['start_time'].max()) - pd.Timedelta("14 day")).strftime('%m/%d/%Y')

    # filtering by dates greater than or equal to the start time 
    dailyPopularity = dailyPopularity.loc[dailyPopularity['start_time'] >= maxDate]
    
    # group by to find the 3 most popular stations by day for the last two weeks; 
    dailyPopularity = dailyPopularity[['start_time',
    'day_of_week','from_station_name','trip_id']].groupby(['start_time',
    'from_station_name','day_of_week'], as_index=False).agg(
        most_popular_station=('trip_id','count')
    ).sort_values(['start_time','most_popular_station']).groupby('start_time').tail(3)


    # Do Males or Females take longer trips on average?
    genderStats = q19.copy(); 

    genderStats = genderStats.loc[((genderStats['gender'] != '') & (genderStats['birthyear'] != ''))]

    # # dataframe for trips length; 
    genderStats = genderStats[['gender',
    'tripduration']].groupby('gender', 
    as_index=False).mean().round(2).assign(avg_trip_in_minutes=lambda avgMinutes: (avgMinutes['tripduration'] / 60).round(2)).applymap(str).assign(avg_trip_in_minutes= lambda newForm: (newForm['avg_trip_in_minutes'].str.replace(r'.',':', regex=True))).drop(columns=['tripduration'])


    print(genderStats)

    # What is the top 10 ages of those that take the longest trips, and shortest?

    pass
if __name__ == '__main__':
    main()