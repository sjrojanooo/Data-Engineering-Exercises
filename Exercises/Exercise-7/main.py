import requests; 
from bs4 import BeautifulSoup # importing beautiful soup package html parser; 
from io import BytesIO, StringIO; 
from zipfile import ZipFile;
from datetime import datetime; 

# target url; 
baseURL = 'https://grouplens.org/datasets/movielens/'

def main():

    # initial request
    in_memory_html = request_page(baseURL);
    # soupt parser
    soup = beautiful_soup(in_memory_html); 

    # scraped html page, and capturing the zip file target element
    in_memory_zip = request_page(filter_href_target(soup, "ml-25m.zip"))

    with ZipFile(in_memory_zip) as zippy: 

        print(zippy)

        for item in zippy.infolist():

            if item.filename.endswith(".csv") and item.filename == "ml-25m/movies.csv":

                with open('./data/movies.csv',"w") as f:

                    for row in zippy.open(item):

                        f.write(row.decode("utf-8"))

# target base url
def request_page(url: str):

    r = requests.get(url)

    if r.status_code == 200: 

        print("successful request"); 

        return BytesIO(r.content)

    elif r.status_code == 400: 

        print("Bad Response");

# beautiful soup parser; 
def beautiful_soup(responseUrl: object):
    
    soup = BeautifulSoup(responseUrl, "html.parser"); 

    table_data = soup.find_all("li"); 

    return table_data


# filter row based on html element; 
def filter_href_target(a_tags: object, filterString: str):

    href_target =  [row.find("a") for row in a_tags if all(elements in row.text for elements in filterString)][0].get("href")

    return href_target; 


if __name__ == '__main__': 

    t1 = datetime.now(); 

    main()

    t2 = datetime.now(); 

    x = t1 - t2

    print(f'It took {x} to process script')