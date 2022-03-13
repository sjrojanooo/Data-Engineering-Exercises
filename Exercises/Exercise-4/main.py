from fileinput import filename
import pandas as pd; 
import os;
import json; 
import re; 


def main():

    # getting into the root directory; 
    presentDir = os.path.abspath('')

    # general crawling; 
    for root, dir_names, file_names in os.walk(presentDir):

        # iteration over all the filenames in the projects directory; 
        for f in file_names:

            # fileName is equalt to the complete address to each file; 
            fileName = os.path.join(root, f);

            # conditional statements that will find all files that end with the json extension; 
            if fileName.endswith('.json'):

                with open(fileName) as jsonFile:

                    data = json.load(jsonFile);

                    data = pd.json_normalize(data);

                    # split the geolocation columns nested list into two separate columns

                    data['geolocation.lon'], data['geolocation.lat'] = map(list, zip(*data['geolocation.coordinates']));

                    data = data.drop(columns='geolocation.coordinates');

                    filteredList = [x for x in fileName.split('/') if any(file in x for file in ['json'])]

                    csvFileName = filteredList[0]
                    data.to_csv(f'./csvData/{csvFileName}.csv', index=False)

                    print(data)

            else: 
                print('not a json')

if __name__ == '__main__':
    main()