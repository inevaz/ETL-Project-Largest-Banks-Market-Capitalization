# ETL-Project-Largest-Banks-Market-Capitalization
This project performs a complete Extract, Transform, and Load (ETL) pipeline to analyze the market capitalization of the largest banks in the world. It scrapes data from Wikipedia, transforms it using exchange rates, and stores the results in both CSV format and an SQLite database.
Academic Project – IBM Data Engineering Course

- Project Description
The script does the following:
Extract : Scrapes a table of the world's largest banks from a Wikipedia page .
Transform : Converts the market capitalization values from USD to GBP, EUR, and INR using exchange rate data.
Load : Saves the transformed data into a CSV file and loads it into an SQLite database.
Query : Executes SQL queries to retrieve insights like average market cap and top bank names.
Visualize : Includes an interactive data visualization interface built with Streamlit , allowing users to explore the dataset through charts and tables directly in a web browser.
- Features
Web scraping with BeautifulSoup
Currency conversion using exchange rates
Data storage in both CSV and SQLite
SQL querying for data analysis
Logging system for tracking execution steps
Interactive data visualization dashboard using Streamlit
- Technologies Used
Python
Pandas – For data manipulation
NumPy – For numerical operations
Requests & BeautifulSoup – For web scraping
SQLite – For local relational database management
datetime – For logging timestamps
Streamlit – For building the data visualization interface

* The visualization part with Streamlit was not part of the IBM course, it was personal implementation.
