import requests
import concurrent.futures
import aiohttp
import asyncio
import glob 
import os
import pandas as pd 
import zipfile



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

    with requests.get(downloadURI, stream=True, timeout=timeout) as r: 
        with open(filePath, "wb") as f: 
            for chunk in r.iter_content(chunk_size=16*1024): 
                f.write(chunk)



# Using the ThreadPoolExecutor to download the files provided in the list; 
def handleZipFileData(endpointList):

    with concurrent.futures.ThreadPoolExecutor() as executor:

        futures = []
        filePath = ''; 

        for url in endpointList:
            filePath = f'./downloads/'+url.split('/')[3] 
            futures.append(executor.submit(requestData, downloadURI=url, filePath=filePath))
        

def main():

    handleZipFileData(download_uris)
 
    pass

if __name__ == '__main__':
    main()
