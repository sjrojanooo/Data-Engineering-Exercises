from numpy import dtype
from pyspark.sql import SparkSession;
import pandas as pd; 
import numpy as np; 
import zipfile; 
import os; 
import heapq; 
import glob; 


# method to walk the data directory and return the path endpoint to each zip file; 
def walkDataDir(presentDirectory):

    dataPath = f'{presentDirectory}/data'; 

    zippedFiles = []; 

    for root, dir_names ,fileNames in os.walk(dataPath):

        for f in fileNames: 

            zippedFiles.append(os.path.join(root,f))

    return zippedFiles; 

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

    # formatting the start and end time columns as mm/dd/yyyy format; 
    q19['start_time'] = pd.to_datetime(q19['start_time']).dt.strftime('%m/%d/%Y');

    q19['end_time'] = pd.to_datetime(q19['end_time']).dt.strftime('%m/%d/%Y');

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

    stationPopularity['month'] = pd.to_datetime(stationPopularity['start_time']).dt.month_name()

    first_position = stationPopularity.pop('month');

    stationPopularity.insert(1, 'month', first_position);

    popularityByMonth = stationPopularity.copy()
    # three most popular stations for each month; 
    popularityByMonth = popularityByMonth[['month',
        'from_station_name','trip_id']].groupby(['month','from_station_name'],
        as_index=False).agg(
        most_popular_month=('trip_id','count')
    ).sort_values(['month','most_popular_month'], 
    ascending=False).groupby('month', 
    as_index=False).first()

    # most popular stations for each day the last two weeks; 
    dailyPopularity = stationPopularity.copy()

    maxDate= pd.to_datetime(pd.Timestamp(dailyPopularity['start_time'].max()) - pd.Timedelta("14 day")).strftime('%m/%d/%Y')

    dailyPopularity = dailyPopularity.loc[dailyPopularity['start_time'] >= maxDate]

    dailyPopularity['day_of_week'] = pd.to_datetime(dailyPopularity['start_time']).dt.day_name(); 
    
    dailyPopularity = dailyPopularity[['start_time',
    'day_of_week','from_station_name','trip_id']].groupby(['start_time',
    'from_station_name','day_of_week']).agg(
        most_popular_day=('trip_id','count')
    )

    dailyPopularity.to_csv('file-check.csv'); 

    # .apply(lambda x: x.nlargest(21)).sort_values(['start_time',
    # 'day_of_week']).reset_index()


    print(dailyPopularity)


    pass
if __name__ == '__main__':
    main()