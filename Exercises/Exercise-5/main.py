import psycopg2; # PostgreSql database adapter for python;
import pandas as pd; # Pythong library for working with datasets; 
from dotenv import dotenv_values; # loading all dotenv variables; 

# creating a dictionary from all the variables located in my .env file; 
config = {**dotenv_values('.env')}

def main():

    # connection to postgres database 
    host=config['DATABASE_HOST']
    database=config['DATABASE_NAME']
    user=config['DATABASE_USERNAME']
    pas=config['DATABASE_PASSWORD']
    port=config['DATABASE_PORT']
    conn = psycopg2.connect(
        host=host, 
        database=database, 
        user=user, 
        password=pas,
        port=port
        )

    conn.autocommit = True;

    # cursor will be used to create tables; 
    cursor = conn.cursor()

    print(cursor)

    # accounts data;
    accountsDf = pd.read_csv('./data/accounts.csv');

    # products data;
    productsDf = pd.read_csv('./data/products.csv');
 
     # products data;
    transactionsDf = pd.read_csv('./data/transactions.csv');

    print('account column names')
    print('---------------------') 
    print(accountsDf.columns);

    print('products column names') 
    print('---------------------') 
    print(productsDf.columns); 

    print('transactions column names') 
    print('---------------------') 
    print(transactionsDf.columns)

    # sql script to create the tables that correspond to each csv file; 
    # moving this into another file later; 
    accountsTable = """CREATE TABLE account(
            customer_id INTEGER, 
            first_name VARCHAR(50), 
            last_name VARCHAR(50),
            address_1 VARCHAR(100),
            address_2 VARCHAR(100), 
            city VARCHAR(50), 
            city_state VARCHAR(50),
            zip_code varchar(50), 
            join_date DATE,
            PRIMARY KEY(customer_id)
            INDEX cust_idx ON (customer_id)
            )
        """


    productsTable = """CREATE TABLE product(
        product_id INTEGER, 
        product_code INTEGER,
        product_description VARCHAR(150), 
        PRIMARY KEY(product_id)
    ) """

    transactionTable = """CREATE TABLE transaction(
        transaction_id VARCHAR(150), 
        transaction_date DATE,
        product_id INTEGER, 
        product_code INTEGER,
        product_description VARCHAR(150), 
        quantity INTEGER , 
        account_id INTEGER,
        PRIMARY KEY(transaction_id),
        FOREIGN KEY(product_id) REFERENCES product (product_id),
        FOREIGN KEY(account_id) REFERENCES account (customer_id)
    ) """


    # DDL statement to copy all csv data over to the database; 
    insertAccounts = """copy account(customer_id, first_name, last_name, address_1,address_2, city, city_state, zip_code, join_date)\
        FROM '/Users/stevenrojano/Data-Engineering-Exercises/Exercises/Exercise-5/data/accounts.csv' 
        DELIMITER ',' 
        CSV HEADER;"""

    insertProducts= """copy product(product_id, product_code, product_description)\
        FROM '/Users/stevenrojano/Data-Engineering-Exercises/Exercises/Exercise-5/data/products.csv' 
        DELIMITER ',' 
        CSV HEADER;"""

    insertTransactions= """copy transaction(transaction_id, transaction_date, product_id, product_code,product_description, quantity, account_id)\
        FROM '/Users/stevenrojano/Data-Engineering-Exercises/Exercises/Exercise-5/data/transactions.csv' 
        DELIMITER ',' 
        CSV HEADER;"""

    # executing sql table creations;
    cursor.execute(accountsTable)
    cursor.execute(productsTable)
    cursor.execute(transactionTable)

    # copying data from csv files into the database; 
    cursor.execute(insertAccounts)
    cursor.execute(insertProducts)
    cursor.execute(insertTransactions)

    # commiting all executions;
    conn.commit()
    # closing the connection; 
    conn.close()


if __name__ == '__main__':
    main(); 

