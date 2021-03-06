import pandas as pd # importing the pandas package 
import requests # request package to use to make http requests; 
from bs4 import BeautifulSoup # importing beautiful soup package html parser; 

# I will be coming back to put some parts of the code into functions, and capturing all files 
# with the target timestamp. Next, I will concantenate all and put them into one data frame and csv file; 


# base url endpoint that we are targetting;
baseURL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/';


# function that will create the data frame; 
def createDataFrame(csvFile, column, fileName):

            dataFrame = pd.read_csv(csvFile)

            # obtaining the list of columns from the obtained information;
            print(dataFrame.columns)
            # printing the data types that I am dealing with; 
            print(dataFrame.info())


            # sorting all column in descending order; 
            dataFrame = dataFrame.sort_values(column, ascending=False);

            # printing all unique values for HourlyDryBulbTemperature; 
            print(dataFrame[column].unique());

            # moving the column into the 1st position for easy observation; 
            third_column = dataFrame.pop(column);

            dataFrame.insert(2, column, third_column);

            # Setting the data frame equal to HourlyDryBulbTemperature that is greater or equal than 40
            dataFrame = dataFrame.loc[dataFrame[column] >= 40]

            # writing out the data frame to the downloads directory; 
            dataFrame.to_csv(f'./downloads/{fileName}.csv', index=False); 

            print(dataFrame);


def getData(url):

    with requests.get(url) as r:

        if r.status_code == 200:

            print('successfull connection');
            print('----------------------');

            # creating an instance of Beautiful soups html parser; 
            soup = BeautifulSoup(r.text, 'html.parser');

            # finding the table element in the document; 
            table = soup.find('table');

            # variable to hold our rows; 
            rows = table.find_all('tr');

            myList=[]; 
            # user can input this value at the beginning of the program; 
            myFilter = ['2022-02-07 14:03'];

            # iterating over the html table containg all of the csv files and timestamps;
            for i in rows: 

                table_data = i.find_all('td');

                data = [j.text.strip() for j in table_data];

                myList.append(data);


            # returning a filter of all items containing the timestamp objective; 
            result = [row for row in myList if all(element in row for element in myFilter)];

            # empty list that will flatten the nested array; 
            flattenList = []; 

            for element in result: 

                for item in element: 
                    flattenList.append(item)


            # filtering the list for all csv instances, since the timestamp was not a unique value; 
            flattenList = [s for s in flattenList if '.csv' in s]


            # empty list to hold my csv file; 
            csvLink = []; 

            # finally, finding all a tags and returning the href element links to csv files; 
            for x in soup.find_all('a'):

                # Conditionally capturing the first occurrence of the target timestamp; 
                if x.text == flattenList[0]:

                    fileLink = baseURL+x.get('href')

                    csvLink.append(fileLink)


            # Calling custom function from above; 
            # three parameters are the csv file link, column that will provide the data for records with highest temp, 
            # and the name of the file; 
            createDataFrame(csvLink[0], 'HourlyDryBulbTemperature', 'station-dry-bulb-temp')

        elif r.status_code == 400:

            print('Unsuccessful connection'); 

def main(): 

    getData(baseURL)
    pass

if __name__ == '__main__':
    main()