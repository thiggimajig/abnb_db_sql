#!/usr/bin/env python3
#just want to add mysql to the path so i can access it from terminal and investigate but should also be able to use the gui ... 
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
#create_database(connection,create_database_query)

#this function is for all queries very similar to create database except uses .commit() to make sure sql queries held in strings in python are implemented 
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

# create noun tables
# account for the 1 to many company account relationship 
create_accounts_table = """
CREATE TABLE accounts (
  account_id INT PRIMARY KEY,
  account_number INT UNIQUE,
  account_type VARCHAR(40) NOT NULL,
  company INT
  );
 """
# execute_query(connection, create_accounts_table) # Execute our defined query
#alter
alter_accounts_table = """
ALTER TABLE accounts
ADD FOREIGN KEY(company)
REFERENCES company(company_id)
ON DELETE SET NULL;
"""
# execute_query(connection, alter_accounts_table) # Execute our defined query
# host
create_hosts_table = """
CREATE TABLE hosts (
  host_id INT PRIMARY KEY,
  host_name VARCHAR(40) NOT NULL,
  review_rating INT,
  superhost_status BOOLEAN,
  account INT
  );
 """
# execute_query(connection, create_hosts_table) # Execute our defined query
#alter
alter_hosts_table = """
ALTER TABLE hosts
ADD FOREIGN KEY(account)
REFERENCES account(account_id)
ON DELETE SET NULL;
"""
# execute_query(connection, alter_hosts_table) # Execute our defined query

# guest
create_guests_table = """
CREATE TABLE guests (
  guest_id INT PRIMARY KEY,
  guest_name VARCHAR(40) NOT NULL,
  review_rating INT,
  account INT
  );
 """
# execute_query(connection, create_guests_table) # Execute our defined query
#alter
alter_guests_table = """
ALTER TABLE guests
ADD FOREIGN KEY(account)
REFERENCES account(account_id)
ON DELETE SET NULL;
"""
# execute_query(connection, alter_guests_table) # Execute our defined query

# company
create_company_table = """
CREATE TABLE company (
  company_id INT PRIMARY KEY,
  company_name VARCHAR(40) NOT NULL
  );
 """
# execute_query(connection, create_company_table) # Execute our defined query

# claim
create_claims_table = """
CREATE TABLE claims (
  claim_id INT PRIMARY KEY,
  claim_content VARCHAR(40) NOT NULL,
  host INT
  );
 """
#execute_query(connection, create_claims_table) # Execute our defined query

#alter
alter_claim_table = """
ALTER TABLE claims
ADD FOREIGN KEY(host)
REFERENCES host(host_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_claim_table) # Execute our defined query

# complaint
create_complaint_table = """
CREATE TABLE complaint (
  complaint_id INT PRIMARY KEY,
  complaint_content VARCHAR(40) NOT NULL,
  guest INT
  );
 """
#execute_query(connection, create_complaint_table) # Execute our defined query
#alter
alter_complaint_table = """
ALTER TABLE complaint
ADD FOREIGN KEY(guest)
REFERENCES guest(guest_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_complaint_table) # Execute our defined query


# government
create_government_table = """
CREATE TABLE government (
  government_id INT PRIMARY KEY,
  government_name VARCHAR(40) NOT NULL
  );
 """
# execute_query(connection, create_government_table) # Execute our defined query

# listing
create_listing_table = """
CREATE TABLE listings (
  listing_id INT PRIMARY KEY,
  listing_name VARCHAR(40) NOT NULL,
  room_type VARCHAR(40) NOT NULL,
  availability BOOLEAN NOT NULL,
  address VARCHAR(100) NOT NULL,
  guest_cap INT,
  cancellation_policy BOOLEAN, 
  amenities BOOLEAN, 
  price INT,
  host INT
  );
 """
#execute_query(connection, create_listing_table) # Execute our defined query
#alter
alter_listing_table = """
ALTER TABLE listings
ADD FOREIGN KEY(host)
REFERENCES host(host_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_listing_table) # Execute our defined query

# reservation_price
create_res_price_table = """
CREATE TABLE res_price (
  res_price_id INT PRIMARY KEY,
  amount INT NOT NULL,
  guest INT,
  company INT
  );
 """
#execute_query(connection, create_res_price_table) # Execute our defined query

#alter
alter_res_price_table = """
ALTER TABLE res_price
ADD FOREIGN KEY(guest)
REFERENCES guest(guest_id)
ON DELETE SET NULL,
ADD FOREIGN KEY(company)
REFERENCES company(company_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_res_price_table) # Execute our defined query

# review
create_review_table = """
CREATE TABLE review (
  review_id INT PRIMARY KEY,
  review_rating INT,
  review_content VARCHAR(1000) NOT NULL,
  guest INT,
  host INT
  );
 """
#execute_query(connection, create_review_table) # Execute our defined query
#alter
alter_review_table = """
ALTER TABLE review
ADD FOREIGN KEY(host)
REFERENCES host(host_id)
ON DELETE SET NULL,
ADD FOREIGN KEY(guest)
REFERENCES guest(guest_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_review_table) # Execute our defined query

# wishlist
create_wishlist_table = """
CREATE TABLE wishlist (
  wishlist_id INT PRIMARY KEY,
  number_listings INT,
  guest INT
  );
 """
#execute_query(connection, create_wishlist_table) # Execute our defined query
#alter
alter_wishlist_table = """
ALTER TABLE wishlist
ADD FOREIGN KEY(guest)
REFERENCES guest(guest_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_wishlist_table) # Execute our defined query

# host_earnings
create_host_earnings_table = """
CREATE TABLE host_earnings (
  host_earnings_id INT PRIMARY KEY,
  amount INT,
  company INT,
  host INT
  );
 """
#execute_query(connection, create_host_earnings_table) # Execute our defined query

alter_host_earnings_table = """
ALTER TABLE host_earnings
ADD FOREIGN KEY(host)
REFERENCES host(host_id)
ON DELETE SET NULL,
ADD FOREIGN KEY(company)
REFERENCES company(company_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_host_earnings_table) # Execute our defined query

# company_profit
create_company_profit_table = """
CREATE TABLE company_profit (
  company_profit_id INT PRIMARY KEY,
  amount INT,
  company INT
  );
 """
#execute_query(connection, create_company_profit_table) # Execute our defined query
#alter
alter_company_profit_table = """
ALTER TABLE company_profit
ADD FOREIGN KEY(company)
REFERENCES company(company_id)
ON DELETE SET NULL;
"""
#execute_query(connection, alter_company_profit_table) # Execute our defined query

# gov_taxes
create_gov_taxes_table = """
CREATE TABLE gov_taxes (
  gov_taxes_id INT PRIMARY KEY,
  amount INT,
  company INT,
  government INT
  );
 """
# execute_query(connection, create_gov_taxes_table) # Execute our defined query

alter_gov_taxes_table = """
ALTER TABLE gov_taxes
ADD FOREIGN KEY(company)
REFERENCES company(company_id)
ON DELETE SET NULL,
ADD FOREIGN KEY(government)
REFERENCES government(government_id)
ON DELETE SET NULL;
"""
# execute_query(connection, alter_gov_taxes_table) # Execute our defined query


#create verb tables 
#guest reserves listing  many to many relationship
guest_reserves_listing_table = """
CREATE TABLE guest_reserves_listing (
  guest_id INT,
  listing_id INT,
  PRIMARY KEY(guest_id, listing_id),
  FOREIGN KEY(guest_id) REFERENCES guest(guest_id) ON DELETE CASCADE,
  FOREIGN KEY(listing_id) REFERENCES listing(listing_id) ON DELETE CASCADE
);
"""

# execute_query(connection, guest_reserves_listing_table) # Execute our defined query

#now let's populate some data

pop_account = """
INSERT INTO accounts VALUES
(1,  12345, 'host', 1),
(2, 23456, 'guest', 1), 
(3, 34567, 'host', 1),
(4,  67890, 'guest', 1);
"""
# execute_query(connection, pop_account) # Execute our defined query

pop_host = """
INSERT INTO hosts VALUES
(1,  'Jack', 9, TRUE, 1),
(2, 'Jim', 1, FALSE, 2);
"""
#execute_query(connection, pop_host) # Execute our defined query

pop_guest = """
INSERT INTO guests VALUES
(1,  'Nora', 5, 2),
(2, 'Jasmine',  2, 4);
"""

#execute_query(connection, pop_guest) # Execute our defined query

pop_company = """
INSERT INTO company VALUES
(1,  'airbnb');
"""
# execute_query(connection, pop_company) # Execute our defined query
pop_claim = """
INSERT INTO claims VALUES
(1, "broken windoww", 1),
(2, "carpet stain", 2);
"""
#execute_query(connection, pop_claim)

pop_complaint = """
INSERT INTO complaint VALUES
(1, "bed bugs", 1),
(2, "mold", 2);
"""
#execute_query(connection, pop_complaint)

pop_government = """
INSERT INTO government VALUES
(1, "State of Ohio"),
(2, "Government of Slovakia")
"""
#execute_query(connection, pop_government)

pop_listing = """
INSERT INTO listings VALUES
(1, "Cute cottage in the woods", "Entire", TRUE, "123 Main street, Cleveland, Ohio, Usa", 5, FALSE, TRUE, 200, 1),
(2, "Chic funky loft in Bratislava", "Shared", FALSE, "10 Bartokova ulica, Sturovo", 2, TRUE, FALSE, 50, 2)
"""
#execute_query(connection, pop_listing)

pop_res_price = """
INSERT INTO res_price VALUES
(1, 250, 1, 1),
(2, 75, 2, 1),
(3, 230, 2, 1)
"""
#execute_query(connection, pop_res_price)

pop_review = """
INSERT INTO review VALUES
(1, 5, "Great stay", 2,2),
(2, 4, "Superb", 1, 1)
"""
# execute_query(connection, pop_review)

pop_wishlist = """
INSERT INTO wishlist VALUES
(1, 6, 1)
"""
#execute_query(connection, pop_wishlist)

pop_host_earnings = """
INSERT INTO host_earnings VALUES
(1, 100, 1, 1),
(2, 30, 1, 2)
"""
#execute_query(connection, pop_host_earnings)

pop_profits = """
INSERT INTO company_profit VALUES
(1, 50, 1),
(2, 10, 1)
"""
#execute_query(connection, pop_profits)

pop_taxes = """
INSERT INTO gov_taxes VALUES
(1, 10, 1, 1),
(2, 5, 1, 2)
"""
#execute_query(connection, pop_taxes)

pop_reserves = """
INSERT INTO guest_reserves_listing VALUES
(1, 1),
(2, 2)
"""
#execute_query(connection, pop_reserves)

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


q0 = """
SELECT *
FROM listings;
"""

q1 = """
SELECT *
FROM guests;
"""

q2 = """
SELECT *
FROM company;
"""

q3 = """
SELECT *
FROM hosts;
"""

q4 = """
SELECT *
FROM accounts;
"""
q5 = """
SELECT * 
FROM guest_reserves_listing;
"""
q6 =  """
SELECT guest_id, guest_name, review_rating
FROM guests
ORDER BY review_rating ASC;
"""
q7 =  """
SELECT listing_name, room_type, address, price, host
FROM listings
ORDER BY price DESC;
"""

q8 = """
SELECT count(*) AS accountCount FROM accounts; 
"""
q9 = """
SELECT host_name, AVG(review_rating)
FROM hosts
GROUP BY host_name
"""

# q9 = """
# SELECT hosts.host_name, hosts.review_rating
# FROM hosts 
# JOIN listings
# ON host.host_id = listings.host
# WHERE listings.availability = FALSE;
# """

update_guest = """
UPDATE guests 
SET guest_name = 'Jazzy' 
WHERE guest_id = 2;
"""
delete_guest = """
DELETE FROM guests 
WHERE guest_id = 2;
"""
#make_change = read_query(connection, update_guest)

results = read_query(connection, q9)
for result in results:
  print(result)

# Returns a list of lists and then creates a pandas DataFrame
# from_db = []

# for result in results:
#   result = list(result)
#   from_db.append(result)

# columns = ["id", "name", "rating"]
# df = pd.DataFrame(from_db, columns=columns)
# print(df)