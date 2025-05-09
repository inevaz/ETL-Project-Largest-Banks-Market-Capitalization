import streamlit as st
import pandas as pd
import sqlite3

#title of the app
st.title("Visualización de Datos de Bancos")

#loading data from csv file
df = pd.read_csv('bank_market_cap_gbp_eur_inr.csv')

#showing the first 5 rows of the dataframe
st.subheader("Tabla de Datos")
st.dataframe(df)

#showing the shape of the dataframe
st.subheader("Estadísticas")
st.write(df.describe())

#ddding a bar chart for Market Cap in USD
st.subheader("Gráfico de Market Cap en USD")
st.bar_chart(df[['Bank name', 'MC_USD_Billion']].set_index('Bank name'))

#connecting to the SQLite database
conn = sqlite3.connect('Banks.db')
query = "SELECT * FROM Largest_banks"
df_db = pd.read_sql(query, conn)

#showing the data from the database
st.subheader("Datos desde la Base de Datos")
st.dataframe(df_db)

#closing the connection to the database
conn.close()