#import libraries
import pandas as pd
import numpy as np
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

#URLs and files
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
target_file = 'bank_market_cap.csv'
exchange_rate_file = 'exchange_rate.csv'
log_file = 'code_log.txt'

#function to log progress
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S' #changed from '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f'{timestamp} : {message}\n')

#function to extract data
def extract(url):
    log_progress("Starting data extraction")
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('table')
    table_index = 2
    table = tables[table_index]

    columns = ["Bank name", "Market capitalization(US$million)"]
    df = pd.DataFrame(columns=columns)

    for row in table.find_all('tr')[1:]:
        col = row.find_all('td')
        if len(col) >= 3:
            bank_name = col[1].get_text(strip=True)
            market_cap_text = col[2].get_text(strip=True).replace(',', '')

            try:
                market_cap = float(market_cap_text)
                df = pd.concat([df, pd.DataFrame([{
                    "Bank name": bank_name,
                    "Market capitalization(US$million)": market_cap
                }])], ignore_index=True)
            except ValueError:
                pass #ignore error
        else:
            pass #ignore rows with few columns
    log_progress("Data extraction complete")
    return df

#function to transform data
def transform(df):
    log_progress("Starting data transformation")

    #read exchange rate file
    exchange_rates = pd.read_csv(exchange_rate_file, index_col='Currency')

    #convert to dictionary: {'GBP': 0.73, 'EUR': 0.89, 'INR': 74.12}
    exchange_rate_dict = exchange_rates['Rate'].to_dict()

    #add new columns to DataFrame, converting USD to other currencies
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict.get('GBP', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict.get('EUR', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict.get('INR', 1), 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete")
    
    return df

#function to load data to csv
def load_to_csv(df, file_path):
    log_progress("Starting data load to CSV")
    df.to_csv(file_path, index=False)
    log_progress("Data successfully saved to CSV")

#function to load data to database
def load_to_db(df, conn, table_name):
    log_progress("Starting data load to database")
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data successfully loaded to database")

#function to run SQL query
def run_query(query_statement, sql_connection):
    log_progress(f"Executing query: {query_statement}")
    result = pd.read_sql(query_statement, sql_connection)
    print(f"\nQuery result:\n{query_statement}")
    print(result)
    return result

#start of the process
log_progress("Preliminaries complete. Starting ETL process")

#1. Extraction
df_extracted = extract(url)
print("Extracted data:")
print(df_extracted.head())

#2. Save to CSV
df_extracted.to_csv(target_file, index=False)
log_progress("Data saved to CSV file")

#3. Load from CSV for transformation
df_transform = pd.read_csv(target_file)

#clean column names (in case of invisible spaces)
df_transform.columns = df_transform.columns.str.strip()

#rename column only if it exists
original_col_name = 'Market capitalization(US$million)'
if original_col_name in df_transform.columns:
    df_transform.rename(columns={original_col_name: 'MC_USD_Billion'}, inplace=True)
else:
    raise KeyError(f"Column '{original_col_name}' not found in CSV.")

#4. Transformation
df_final = transform(df_transform)
print("\nTransformed data:")
print(df_final.head())

#5. Show requested value
print("Value of df['MC_EUR_Billion'][4]:", df_final['MC_EUR_Billion'][4])

#5. Save transformed data to CSV
output_csv = 'bank_market_cap_gbp_eur_inr.csv'
load_to_csv(df_final, output_csv)
print(f"File saved as: {output_csv}")
 
#start SQLite connection
log_progress("Starting database connection")
conn = sqlite3.connect('Banks.db')
log_progress("Connection successfully established")

#call function to load data
table_name = 'Largest_banks'
load_to_db(df_final, conn, table_name)

#6. SQL queries
log_progress("Starting SQL query execution")

#query 1: show all table content
run_query("SELECT * FROM Largest_banks", conn)

#query 2: average market cap in GBP
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)

#query 3: names of the first 5 banks
run_query('SELECT "Bank name" FROM Largest_banks LIMIT 5', conn)

#close connection
conn.close()
log_progress("Database connection closed")

log_progress("Process complete")