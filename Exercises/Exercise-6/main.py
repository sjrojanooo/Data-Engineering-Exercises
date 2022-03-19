from datetime import date
from pyspark.sql import SparkSession;
import pyspark.sql.functions as F;
import pandas as pd; 
import numpy as np; 
import pyspark.pandas as ps
import zipfile; 
import os; 


spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()

# method to walk the data directory and return the path endpoint to each zip file; 
def walkDataDir(presentDirectory):

    dataPath = f'{presentDirectory}/data'; 

    zippedFiles = []; 

    for root, dir_names ,fileNames in os.walk(dataPath):

        for f in fileNames: 

            zippedFiles.append(os.path.join(root,f))

    return zippedFiles; 

# method to read zip contents; 
def readZipContents(data_dirname,response,startTimeCol,tripDurationCol):

    zipResponse = walkDataDir(data_dirname)    

    zip_file = zipfile.ZipFile(zipResponse[response])

    df = pd.read_csv(zip_file.open(zip_file.namelist()[0]),
        thousands=',',
            decimal='.',
            )

    df[tripDurationCol] = (df[tripDurationCol] / 60).round(2)

    df = ps.DataFrame(df)

    return df

def averageTripsAndTotalTrips(dataFrame, startTimeCol, tripIdCol, tripDurCol):
    # Average Trip duration and total trips taken per day; 
    dataFrame = dataFrame[[startTimeCol,tripIdCol,tripDurCol

        ]].assign(startTimeCol= lambda x: (ps.to_datetime(x[startTimeCol]).dt.strftime('%m/%d/%Y'))).groupby(startTimeCol).agg(

            daily_trip_count=(tripIdCol,'count'),

                average_trip_duration=(tripDurCol, 'mean')

                ).sort_index();

    dataFrame['average_trip_duration'] = dataFrame['average_trip_duration'].round(2)

    return dataFrame

    
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

    # Quarter 19 contents; 
    quarter19 = readZipContents(os.path.abspath(''),1,'start_time','tripduration');

    # Average trips and total trip count csv file; 
    avgDf = averageTripsAndTotalTrips(quarter19,
    'start_time','trip_id','tripduration'
    );
    
    # most popular trips for each month; 
    monthDf19 = stationPopularityByMonth(quarter19);

    # popularity for the past two weeks; 
    weekly19 = stationPopularityByDay(quarter19);

    # mean tripduration by gender; 
    genderStats = averageTripDurationByGender(quarter19); 

    # longest and shortest trips by age group; 
    longestTrips, shortestTrips = longestShortestTripByAge(quarter19); 


    pass

if __name__ == '__main__':
    main()