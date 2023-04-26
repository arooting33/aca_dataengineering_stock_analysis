import os
from datetime import datetime
from urllib.error import HTTPError

import requests
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from db.db_tools import connect_db


# tasks: convert dict into pd.Series, add pd.Series into dataframe as a row , save dataframe in excel
# turn symbols column into a list to iterate over
company_names_file_name = 'company_names.csv'
resources_folder = 'resources'


def scrape_summary(symbol: str) -> dict:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    url = f'https://finance.yahoo.com/quote/{symbol}'
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    #summary scrape
    fin_streamers = soup.find('div', {'class': 'D(ib) Mend(20px)'}).find_all('fin-streamer')
    tr_list_1 = soup.find('table', {'class': 'W(100%)'}).find_all('tr')
    tr_list_2 = soup.find('table', {'class': 'W(100%) M(0) Bdcl(c)'}).find_all('tr')
    df_stock = {
        'symbol': symbol,
        'price': fin_streamers[0].text,
        'change': fin_streamers[1].text,

    }
    for tr in tr_list_1:
        td_list = tr.find_all('td')
        name = td_list[0].text
        value = td_list[1].text
        df_stock[name] = value
    for tr in tr_list_2:
        td_list = tr.find_all('td')
        name = td_list[0].text
        value = td_list[1].text
        df_stock[name] = value

    return df_stock


def scrape_stats(symbol):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    url_stats = f'https://finance.yahoo.com/quote/{symbol}/key-statistics?p={symbol}'
    r_stats = requests.get(url_stats, headers=headers)
    soup_stats = BeautifulSoup(r_stats.text, 'html.parser')
    table_list = soup_stats.find_all('table', {'class': 'W(100%) Bdcl(c)'})

    df_stock = {'symbol': symbol}

    for table in table_list:
        tr_list = table.find_all('tr')
        for tr in tr_list:
            td_list = tr.find_all('td')
            name = td_list[0].text
            value = td_list[1].text
            df_stock[name] = value


    return df_stock


def df_to_db(conn, cursor, df, table_name):
    # TODO column order should be id, date, ... , symbol
    cols = df.columns
    # col1 = [cols[0]]   # the first element to be used as last
    last_two = cols[-2:]  # takes the last two numbers from the original list
    remaining_cols = cols[:-2]  # takes remaining numbers form original list
    new_cols = list(last_two[::-1]) + list(remaining_cols)
    df = df[new_cols]
    df.columns = ["id", "date", "symbol", "price", "price_change", "previous_close", "open", "bid", "ask", "days_range", "weeks_range", "volume", "avg_volume", "marketcap", "beta_5y_monthly", "pe_ratio", "eps", "earnings_date", "forward_dividend_yield", "ex_dividend", "year_target_est_1", "market_cap", "enterprise_value", "trailing_PE", "forward_pe", "peg_ratio", "price_sales", "price_book", "enterprise_value_over_revenue", "enterprise_value_over_ebitda", "beta", "week_change_percent_52", "spy500_52_week_change", "week_high_52", "week_low_52", "day_moving_average_50", "day_moving_average_200", "month_avg_vol_3", "day_avg_vol_10", "shares_outstanding", "implied_shares_outstanding", "float_8", "percent_held_by_insiders", "percent_held_by_institutions", "shares_short", "short_ratio", "short_percent_float", "short_percent_of_shares_outstanding", "shares_short_prior_month", "forward_annual_dividend_rate", "forward_annual_dividend_yield", "trailing_annual_dividend_rate", "trailing_annual_dividend_yield", "year_average_dividend_yield_5", "payout_ratio", "dividend_date", "previous_dividend_date", "last_stock_split", "last_split_date", "fiscal_year_ends", "recent_quarter", "profit_margin", "operating_margin", "return_assets", "return_equity", "revenue", "revenue_per_share", "quarterly_revenue_growth", "gross_profit", "ebitda", "net_income_avi_common", "diluted_eps", "quarterly_earnings_growth", "total_cash", "total_cash_per_share", "total_debt", "total_debt_over_equity", "current_ratio", "book_value_per_share", "operating_cash_flow", "levered_free_cash_flow"]
    for cols in df.columns:
        df[cols] = df[cols].apply(lambda x: x.replace(',', '').replace('%', '') if type(x) == str else x)
    df.replace('N/A', np.nan, inplace=True)
    l = df.to_numpy()[0]
    tuple_list = [tuple(np.append(l, l[2]))] # x
    new_cols = ','.join(list(df.columns)).lower() + ',company_id'
    query = f"INSERT INTO {table_name}({new_cols}) " \
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
            f"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
            f"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
            f"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
            f" (SELECT id FROM company_names WHERE symbol=%s))"
    try:
        extras.execute_batch(cursor, query, tuple_list)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1


def scrape_and_fill_company_stats(connection, cursor, symbol):
    """1. calls for the creation of dataframe from function: scrape_from_url() - which scrapes and returns a df
   2. manipulate the df, create a tuple to insert and insert statement uses DF_TO_DB(): tuples and inserts using sql and psycopg2"""

    try:
        data_dictionary = scrape_summary(symbol)
        data_dictionary2 = scrape_stats(symbol)
        data_dictionary.update(data_dictionary2)  # merging
        data_dictionary['Date'] = datetime.now().date().strftime("%Y-%m-%d")  # TODO received from function argument
        data_dictionary['Id'] = data_dictionary['symbol'] + '_' + str(data_dictionary['Date'])
        res = pd.DataFrame.from_dict(data_dictionary, orient='index').T
        # time.sleep(1)
        df_to_db(connection, cursor, res, 'all_company_stats')
        count = cursor.rowcount
        print(count, "company" + str(symbol.lower()).strip() + "record inserted successfully into historical table ")
    except HTTPError:
        print(f"ERROR: failed to fetch df for company {symbol}")
        return None


def scrape_and_fill_db(): # not UTC NY TIME?
    connection = connect_db()
    cursor = connection.cursor()

    file_path = os.path.join(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), resources_folder), company_names_file_name)

    symbols = pd.read_csv(file_path)['Symbol'].tolist()

    for symbol in symbols:

        scrape_and_fill_company_stats(connection, cursor, symbol)

    cursor.close()


# work for both functions
if __name__ == '__main__':
    scrape_and_fill_db()




