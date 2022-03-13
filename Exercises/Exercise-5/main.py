import pyscopg2

def main():
    host = 'postgres'
    database='postgres'
    user='postgres'
    pas='postgres'
    conn = pyscopg2.connect(host=host, database=database, user=user, password=pas)

    # code goes here; 


if __name__ == '__main__':
    main(); 

