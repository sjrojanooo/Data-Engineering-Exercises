from numpy import dtype
from pyspark.sql import SparkSession;
import pandas as pd; 
import zipfile; 
import os; 
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

    q19 = pd.read_csv(zip_file.open(zip_file.namelist()[0]), parse_dates=True)

    q19['start_time'] = pd.to_datetime(q19['start_time']).dt.strftime('%m/%d/%Y')

    q19['end_time'] = pd.to_datetime(q19['end_time']).dt.strftime('%m/%d/%Y')

    

    pass
if __name__ == '__main__':
    main()