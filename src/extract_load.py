# import libraries
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

#=================================
# Import of environment variables
#=================================
commodities = ['CL=F', 'GC=F', 'SI=F']

DB_HOST_PROD = os.getenv('DB_HOST_PROD')
DB_PORT_PROD = os.getenv('DB_PORT_PROD')
DB_USER_PROD = os.getenv('DB_USER_PROD')
DB_PASS_PROD = os.getenv('DB_PASS_PROD')
DB_SCHEMA_PROD = os.getenv('DB_SCHEMA_PROD')
DB_NAME = os.getenv('DB_NAME')

database_url = f'postgresql://{DB_USER_PROD}:{DB_PASS_PROD}@{DB_HOST_PROD}:{DB_PORT_PROD}/{DB_NAME}'
engine=create_engine(database_url)


#=================================
# Getting stock prices
#=================================
def get_commodities(ticker, period='5d', interval='1d'):
    ticker_orig = ticker
    ticker = yf.Ticker(ticker)
    data = ticker.history(period=period, interval=interval)[['Close']]
    data["ticker"] = ticker_orig
    return data


#=================================
# Concat tickers
#=================================
def concat_tickers(commodities):
    all_data = []
    
    for ticker in commodities:
        data = get_commodities(ticker)
        all_data.append(data)
    
    return pd.concat(all_data)


#=================================
# Save on database Postgress
#=================================
def save_on_postgres(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)



#=================================
# Main to execute
#=================================
concat_data = concat_tickers(commodities)
save_on_postgres(concat_data)
