#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error
import pandas as pd
import password as pw

password = pw.pw_safety()

def create_db_connection(host_name, user_name, user_password, db_name):
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

connection = create_db_connection("localhost", "root", password, "airbnb")

#to create the databse, only needed once so 
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query) #cursor method is giving us access to blinking light of mysql server terminal window 
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

#here we define a query to create the database 
create_database_query = "CREATE DATABASE airbnb"
# create_database(connection,create_database_query)

#this function is for all queries very similar to create database except uses .commit() to make sure sql queries held in strings in python are implemented 
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#create noun tables
#account for the 1 to many company account relationship 
create_account_table = """
CREATE TABLE account (
  account_id INT PRIMARY KEY,
  account_number INT UNIQUE,
  account_type VARCHAR(40) NOT NULL
  company_id INT FOREIGN KEY 
  );
 """
# execute_query(connection, create_account_table) # Execute our defined query
# host
create_host_table = """
CREATE TABLE host (
  host_id INT PRIMARY KEY,
  host_name VARCHAR(40) NOT NULL,
  review_rating INT,
  superhost_status BOOLEAN
  );
 """
# execute_query(connection, create_host_table) # Execute our defined query
# guest
create_guest_table = """
CREATE TABLE guest (
  guest_id INT PRIMARY KEY,
  guest_name VARCHAR(40) NOT NULL,
  review_rating INT
  );
 """
# execute_query(connection, create_guest_table) # Execute our defined query
# company
create_company_table = """
CREATE TABLE company (
  company_id INT PRIMARY KEY,
  company_name VARCHAR(40) NOT NULL
  );
 """
# execute_query(connection, create_company_table) # Execute our defined query

#create verb tables 
#host creates account many to many relationship
create_hostcreatesaccount_table = """
CREATE TABLE hostcreatesaccount (
  host_id INT,
  account_id INT,
  PRIMARY KEY(host_id, account_id),
  FOREIGN KEY(host_id) REFERENCES host(host_id) ON DELETE CASCADE,
  FOREIGN KEY(account_id) REFERENCES account(account_id) ON DELETE CASCADE
);
"""
#guest creates account many to many relationship 
create_guestcreatesaccount_table = """
CREATE TABLE guestcreatesaccount (
  guest_id INT,
  account_id INT,
  PRIMARY KEY(guest_id, account_id),
  FOREIGN KEY(guest_id) REFERENCES guest(guest_id) ON DELETE CASCADE,
  FOREIGN KEY(account_id) REFERENCES account(account_id) ON DELETE CASCADE
);
"""
#company verifies account 1 to many relationship but made it many to many because couldnt get primary key as company only to work sql syntax
create_companyverifiesaccount_table = """
CREATE TABLE companyverifiesaccount (
  company_id INT,
  account_id INT,
  PRIMARY KEY (company_id, account_id),
  FOREIGN KEY (company_id) REFERENCES company(company_id) ON DELETE CASCADE,
  FOREIGN KEY (account_id) REFERENCES account(account_id) ON DELETE CASCADE
);
"""
# execute_query(connection, create_hostcreatesaccount_table) # Execute our defined query
# execute_query(connection, create_guestcreatesaccount_table) # Execute our defined query
# execute_query(connection, create_companyverifiesaccount_table) # Execute our defined query

#now let's populate some data

pop_account = """
INSERT INTO account VALUES
(1,  12345, 'host'),
(2, 23456, 'guest'), 
(3, 34567, 'host'),
(4,  67890, 'guest');
"""
# execute_query(connection, pop_account) # Execute our defined query

pop_host = """
INSERT INTO host VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

pop_guest = """
INSERT INTO guest VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

pop_company = """
INSERT INTO company VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

pop_companyverifiesaccount= """
INSERT INTO company VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

