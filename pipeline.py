#import dependencies
import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def create_server_connection(host_name, user_name, user_password, port):
    """For connecting to MySQL server"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

pw = "*******"
connection = create_server_connection("localhost", "root", pw, 3306)

def create_database(connection, query):
    """To create a database in MySQL, it requires two arguments"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

database_query = "CREATE DATABASE transactions"
create_database(connection, database_query)

def create_db_connection(host_name, user_name, user_password, db_name, port):
    """Connect to created database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    """Execute query in sql"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

# Assign our SQL command to a python variable using triple quotes to create a multi-line string
create_customers_table = """
CREATE TABLE customers (
  id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  email VARCHAR(40) NOT NULL
  );
 """

create_orders_table = """
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  order_date DATE,
  status VARCHAR(30) NOT NULL
);
 """

create_payments_table = """
CREATE TABLE payments (
  id INT PRIMARY KEY,
  order_id INT,
  payment_method VARCHAR(30) NOT NULL,
  amount INT,
  amount_in_naira INT
);
 """

connection = create_db_connection("localhost", "root", pw, transactions, 3306) # Connect to the Database
execute_query(connection, create_customers_table) # Execute our defined query
execute_query(connection, create_orders_table) # Execute our defined query
execute_query(connection, create_payments_table) # Execute our defined query

#---------------------------------------------------------------------------------------------------------

def using_csv_reader(engine, data, table_name):
    """Using csv_reader() to insert the data from the .csv files into the created tables,
    the function takes in 3 arguments.
    engine: parameter required for sqlalchemy
    data: path in string to the .csv file in the directory
    table_name: the name of the table for the insertion
    """
    try:
        # SQL query to execute
        sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table_name, cols)
        sql = sql.format(table_name)
        # Define the data path
        with open(data) as fh:
            #sub_sample = [next(fh) for x in range(records)]
            reader = csv.reader(fh)
            next(reader)  # Skip first line (headers)
            df = list(reader)
        engine.execute(sql, df)
        print("Data inserted using Using_csv_reader() successfully...")
    except Error as err:
        print("Error while inserting to MySQL", e)

#---------------------------------------------------------------------------------------------------------

def read_query(connection, query):
    """Read data from the database"""
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

t1 = """
SELECT *
FROM customers;
"""

connection = create_db_connection("localhost", "root", pw, transactions, 3306)
results = read_query(connection, t1)

for result in results:
  print(result)
