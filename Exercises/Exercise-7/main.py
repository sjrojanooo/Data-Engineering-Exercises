import requests; 
from bs4 import BeautifulSoup # importing beautiful soup package html parser; 
from io import BytesIO, StringIO; 
from zipfile import ZipFile;
from datetime import datetime; 
from concurrent.futures import ProcessPoolExecutor; 

# target url; 
baseURL = "https://grouplens.org/datasets/movielens/"

# Entry point to data pipeline; 
def main():

    # initial request;
    in_memory_html = request_page(baseURL);

    # creating an instance of beautiful soup and finding all list elements in html page; 
    soup = beautiful_soup(in_memory_html); 

    # filtering all href values and capturing the target zip file; 
    in_memory_zip = request_page(filter_href_target(soup, "ml-25m.zip"))

    # handling the zipfile object and writing zip file contents to data directory; 
    zip_file_contents(in_memory_zip);

# target base url
def request_page(url: str):

    r = requests.get(url)

    if r.status_code == 200: 

        print("successful request"); 

        return BytesIO(r.content);

    elif r.status_code == 400: 

        print("Bad Response");

# beautiful soup parser; 
def beautiful_soup(responseUrl: object):
    
    soup = BeautifulSoup(responseUrl, "html.parser"); 

    table_data = soup.find_all("li"); 

    return table_data;

# filter row based on html element; 
def filter_href_target(a_tags: object, filterString: str):

    href_target =  [row.find("a") for row in a_tags if all(elements in row.text for elements in filterString)][0].get("href");

    return href_target; 


# zipfile method to iterate over contents of zipped file; 
def zip_file_contents(zipObj: object):
        # using zipfile package to interact with target zipfile; 
    with ZipFile(zipObj) as zippy: 

        for item in zippy.infolist():

            if item.filename.endswith(".csv"):

                write_file(zippy, item, file_name(item));

                print(f"file {file_name(item)} complete")
                
# capturing the filename of each object parsed and returning thr string; 
def file_name(fileObj: object):

    return fileObj.filename.split('/')[1];


# write_file to csv; 
def write_file(zippy: object, item: object, fileName: str):
    
    with open(f"./data/{fileName}", "w") as f: 

        for row in zippy.open(item):
            
            f.write(row.decode("utf-8"));


if __name__ == "__main__": 

    t1 = datetime.now(); 

    main();

    t2 = datetime.now(); 

    x = t1 - t2

    print(f'It took {x} to process script')