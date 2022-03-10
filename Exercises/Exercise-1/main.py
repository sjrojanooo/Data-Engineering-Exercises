import os # operating system to manipulate filepaths; 
import glob # glob package finds all pathnames matching a specified pattern;
import requests # request package will be used to make http request;
import concurrent.futures # concurrent future enables the program to perform asynchronus events; 
from zipfile import ZipFile # zipfile package;
from pathlib import Path # used to delete the _MACOSX directory; 



# specified download uri's; 
download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]



# Request method to request the given uri, set a timeout and download the zipfiles; 
def requestData(downloadURI, filePath, timeout=2):

    with requests.get(downloadURI, timeout=timeout) as r: 
        
        if r.status_code == 404:
            print('does not exist')
        elif r.status_code == 200:
            with open(filePath, 'wb') as f: 
                for chunk in r.iter_content(chunk_size=16*1024): 
                    f.write(chunk)

    

# Using the ThreadPoolExecutor to download the files provided in the list; 
def handleZipFileData(endpointList):

    #using thread pool to synchronusly 
    with concurrent.futures.ThreadPoolExecutor() as executor:

        futures = []
        filePath = ''; 

        for url in endpointList:
            filePath = f'./downloads/'+url.split('/')[3] 
            futures.append(executor.submit(requestData, downloadURI=url, filePath=filePath))
    

    # finding all zip files in the downloads directory. 
    for files in glob.glob(os.path.join('./downloads', '*.zip')):

        # creating my zipfile object using the ZipFile package and loading 
        with ZipFile(files, 'r') as zipObj:
            
            fileType = zipObj.namelist()

            for csvFile in fileType: 

                if csvFile.endswith('.csv'): 
                    
                    zipObj.extractall('./downloads')

            print(f'{fileType} has been extracted')

        # removing all zip file after each iteration; 
        # will probably just remove all file after the iteration
        os.remove(files)


    # removes the directory that holds all csv files saved to disk; 
    Path('downloads/__MACOSX').rmdir(); 

    print('csv file extraction complete')



def main():

    handleZipFileData(download_uris)

    pass

if __name__ == '__main__':
    main()
