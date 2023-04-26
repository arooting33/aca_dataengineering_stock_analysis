import time
import os
from datetime import datetime, timedelta
from urllib.error import HTTPError

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from db.db_tools import connect_db

company_names_file_name = 'company_names.csv'
resources_folder = 'resources'
website_url = "https://query1.finance.yahoo.com/v7/finance/download"
INTERVAL = '1d'  # TODO manage for other intervals
# TODO: spacial cases: bigINT issue, Y, BRKA, BRKB, NLOK, NLSN


def df_to_db(conn, cursor, df: pd.DataFrame, table_name: str):
    """
    :param conn:
    :param cursor:
    :param df:
    :param table_name:
    :return:
    """
    df['Id'] = df["symbol"] + "_" + df['Date'] # add new column, DB primary key
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1] # bring primary key to front of column
    df = df[cols]
    tuples_list = [tuple(np.append(x, x[-1])) for x in df.to_numpy()]
    cols = ','.join(list(df.columns)).lower() + ',company_id'
    query = f"INSERT INTO {table_name}({cols}) " \
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, (SELECT id From company_names WHERE symbol=%s))"
    try:
        extras.execute_batch(cursor, query, tuples_list)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1


def get_latest_date_from_db(company_symbol: str, table_name: str, cursor) -> datetime:
    company_symbol = "'" + company_symbol + "'"
    cursor.execute(f"select date from {table_name} where symbol={company_symbol} order by date DESC limit 1")
    res = cursor.fetchone()
    if not res:
        print('No last date found, setting last date to (2000, 01, 01)')
        return datetime(2000, 1, 1, 23, 59)
    last_date = res[0] + timedelta(days=1) # to ensure the latest date is included, otherwise yahoo does not return the latest date
    last_date = datetime.combine(last_date, datetime.max.time())
    return last_date


def download_from_url(ticker: str, period1: int, period2: int) -> pd.DataFrame:
    """Download given company's historical data from yahoo url and return the resulted df
    :param ticker:
    :param period1:
    :param period2:
    :return:
    """
    query_param = ticker.replace('/', '-')
    query_string = f'{website_url}/{query_param}?period1={period1}' \
                   f'&period2={period2}&&interval={INTERVAL}&events=history&includeAdjustedClose=true'
    df = pd.read_csv(query_string)
    df.drop('Adj Close', axis=1, inplace=True)
    df = df.assign(symbol=ticker)
    return df


def scrape_and_fill_a_company(connection, cursor, ticker, period1, period2):
    """Downloads the data for given table_name and period/date
    pulling per company data
    :param connection:
    :param cursor:
    :param period1:
    :param ticker:
    :param period2:
    :return:
    """
    try:
        df = download_from_url(ticker, period1, period2)
        df_to_db(connection, cursor, df, 'all_historical_data')
        count = cursor.rowcount
        print(count, "Record inserted successfully into historical table " + str(ticker.lower()).strip())
    except HTTPError:
        print(f"ERROR: Failed to fetch df for company {ticker}")
        return None


def download_and_fill_db(end_date: datetime, start_date: datetime = None):
    """filling all companies
    :param end_date:
    :param start_date:
    :return:
    """
    connection = connect_db()
    cursor = connection.cursor()
    # this ensures no need for hard coding on other computers, it will work because company_names will be stored in stock_analysis/resources and wont matter the path before that
    file_path = os.path.join(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), resources_folder), company_names_file_name)

    tickers = pd.read_csv(file_path)['Symbol'].tolist()
    end_date = end_date.replace(hour=23, minute=59)  # more robust solution in get last date function
    period2 = int(time.mktime(end_date.timetuple()))

    if start_date:
        start_date = start_date.replace(hour=23, minute=59)
        for ticker in tickers:
            period1 = int(time.mktime(start_date.timetuple()))
            scrape_and_fill_a_company(connection, cursor, ticker, period1, period2)
    else:
        for ticker in tickers:
            start_date = get_latest_date_from_db(ticker, "all_historical_data", cursor)  # skip db's last date
            if start_date.date() == end_date.date():  # verifying up to date date, nothing additional to fetch
                print(f"Last date {str(start_date.date())} is already downloaded. Skipping company {ticker}")
                continue
            period1 = int(time.mktime(start_date.timetuple()))
            scrape_and_fill_a_company(connection, cursor, ticker, period1, period2)
    cursor.close()


if __name__ == '__main__':
    # scrape_and_fill_db(start_date = datetime(2000, 1, 1), end_date = datetime(2023, 1, 30))
    download_and_fill_db(end_date=datetime.now())
