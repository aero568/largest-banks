# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    timestamp_format = "%Y-%h-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    logfile = 'code_log.txt'
    with open(logfile, "a") as f:
        f.write(timestamp + " : " + message + "\n")


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # data frame initialisation
    df = pd.DataFrame(columns=table_attribs[0:2])

    # loading webpage for web scraping
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    # scraping data ( 1st table from URL )
    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")

    # table rows loop
    for row in rows:

        # table data and href extraction from row
        cols = row.find_all("td")
        lnks = row.find_all("a")

        # extract data from valid rows and assign it to data frame
        if len(cols) != 0:
            data_dict = {table_attribs[0]: lnks[1].contents[0],
                         table_attribs[1]: float(cols[2].contents[0].replace(',',''))}
            dfs = [df,pd.DataFrame(data_dict, index=[0])] 
            df  = pd.concat([df for df in dfs  if not df.empty], ignore_index=True)

    # extracted data frame
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    # Read csv file into a data frame ( exchange rates )
    dataframe = pd.read_csv(csv_path)

    # Convert the contents to a dictionary
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    # Adding collumns to data frame
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path) 


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))
    print()

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring know values
url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path='exchange_rate.csv'
csv_path='Largest_banks_data.csv'
table_attributes=['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
database_name='Banks.db'
table_name='Largest_banks'

log_progress('Preliminaries complete. Initiating ETL process')

# Data extraction
df = extract(url,table_attributes)
log_progress('Data extraction complete. Initiating Transformation process')

# Data transformation
df = transform(df, exchange_rate_path)
log_progress('Data transformation complete. Initiating Loading process')

# Data loading to csv file
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

# Data loading to database and queries
sql_connection = sqlite3.connect(database_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

run_query('SELECT * FROM Largest_banks', sql_connection)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_query('SELECT Name from Largest_banks LIMIT 5', sql_connection)
log_progress('Process Complete')

log_progress('Server Connection closed')

