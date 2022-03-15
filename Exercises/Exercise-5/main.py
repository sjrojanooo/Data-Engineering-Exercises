import psycopg2; 
import pandas as pd;
from dotenv import dotenv_values;

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
    accountsTable = """CREATE TABLE accounts(
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
            )
        """


    productsTable = """CREATE TABLE products(
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
        PRIMARY KEY(transactio_id),
        FOREIGN KEY(product_id) REFERENCES products (product_id),
        FOREIGN KEY(account_id) REFERENCES accounts (customer_id)
    ) """



    cursor.execute(accountsTable)
    cursor.execute(productsTable)
    cursor.execute(transactionTable)
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main(); 

