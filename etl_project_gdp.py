# Code for ETL operations on Country-GDP data
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime 
# Importing the required libraries

url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attribs= ['Country','GDP_USD_millions']
db_name = 'World_Economies.db'
table_name ='Countries_by_GDP'
csv_path = 'home/projects/Countries_by_GDP.csv'


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    df=pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        try :
            if row.td.find('a'):
                #print('sisas')
                try :
                    #print(row.find_all('td')[0].a.contents[0],' --> ',int(row.find_all('td')[4].contents[0].replace(',' , '')) )
                    Country = row.find_all('td')[0].a.contents[0]
                    GDP_USD_millions = int(row.find_all('td')[4].contents[0].replace(',' , ''))

                    new_record = pd.DataFrame([{'Country':Country, 'GDP_USD_millions':GDP_USD_millions }])
                    df = pd.concat([df, new_record], ignore_index=True)
                    #print('hereeeeeeeeeeee')
                
                except ValueError as e :
                    # Handle the error if conversion fails
                    #print('except',e)
                    continue
        except AttributeError as e:
            # Handle the error if conversion fails
            #print('except',e)
            continue
        
    return df


print(extract(url,table_attribs))
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

