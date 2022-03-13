from fileinput import filename
import pandas as pd; 
import os;
import json; 

# method to handle Json files and write them to the csvData Directory;
def handleJsonFiles(jsonFile, columnLong, columnLang, columnCoord):

    with open(jsonFile) as jsonObj:

        data = json.load(jsonObj)

        data = pd.json_normalize(data); 

        # split the geolocation columns nested list into two separate columns

        data[columnLong],data[columnLang] = map(list, zip(*data[columnCoord])); 

        data = data.drop(columns=columnCoord)

        filteredList = [x for x in jsonFile.split('/') if (any(file in x for file in ['json']))]

        csvFileName = filteredList[0]

        data.to_csv(f'./csvData/{csvFileName}.csv', index=False)

        print(data)


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

                handleJsonFiles(fileName, 'geolocation.lon', 'geolocation.lat', 'geolocation.coordinates')

            else: 
                print('not a json')

if __name__ == '__main__':
    main()