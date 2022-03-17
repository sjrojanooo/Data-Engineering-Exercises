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

    zip_file = zipfile.ZipFile(zipResponse[0])

    print(zip_file.namelist()[0])

    df = pd.read_csv(zip_file.open(zip_file.namelist()[0]))

    print(df)

    
 
    pass
if __name__ == '__main__':
    main()