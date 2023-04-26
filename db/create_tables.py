import os
import pandas as pd

from db.db_tools import connect_db

company_names_file_name = 'company_names.csv'
resources_folder = 'resources'

company_names_file_path = os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                    resources_folder), company_names_file_name)


# TODO add print statements, function doctrings

# First we create a table with all the company names and basic information
# QUESTION:
def create_company_names_table(table_name: str, connection):
    """
    :param table_name: csv file with company names
    :param connection:
    :return:
    """
    cursor = connection.cursor()
    commands = (
        f""" 
        CREATE TABLE IF NOT EXISTS {table_name}(
           ID SERIAL PRIMARY KEY,
           Symbol char(6) UNIQUE,
           Name char(255),
           Sector char(255),
           Industry	 char(255)
        );
        """,
        f"""
        COPY {table_name} (Symbol,Name,Sector,Industry)
            FROM '{company_names_file_path}'
            DELIMITER ','
            CSV HEADER;
        """
    )
    for command in commands:
        cursor.execute(command)
    cursor.close()
    connection.commit()
    print(f"created {table_name}")


def create_all_company_stats_table(cursor, table_name='all_company_stats'):

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
        id char(32) PRIMARY KEY,
        date date,
        symbol char(6),
        price decimal(18,4),
        price_change decimal(18,4),
        previous_close decimal(18,4),
        open decimal(18,4),
        bid char(24),
        ask char(24),
        days_range char(24),
        weeks_range char(24),
        volume bigint,
        avg_volume bigint,
        marketcap char(14),
        beta_5y_monthly decimal(10,4),
        pe_ratio decimal(10,4),
        eps decimal(10,4),
        earnings_date char(30),
        forward_dividend_yield char(14),
        ex_dividend char(14),
        year_target_est_1 decimal(12,4),
        market_cap char(10),
        enterprise_value char(10),
        trailing_PE decimal(12,4),
        forward_pe decimal(12,4),
        peg_ratio decimal(12,4),
        price_sales decimal(12,4),
        price_book decimal(12,4),
        enterprise_value_over_revenue decimal(12,4),
        enterprise_value_over_ebitda decimal(12,4),
        beta decimal(8,4),                                                     
        week_change_percent_52 char(10),
        spy500_52_week_change char(10),
        week_high_52 decimal(12,4),
        week_low_52 decimal(12,4),
        day_moving_average_50 decimal(12,4),
        day_moving_average_200 decimal(12,4),
        month_avg_vol_3 char(10),
        day_avg_vol_10 char(10),
        shares_outstanding char(10),
        implied_shares_outstanding char(12),
        float_8 char(10),
        percent_held_by_insiders char(10),
        percent_held_by_institutions char(10),
        shares_short char(10),
        short_ratio decimal(6,4),
        short_percent_float char(8),
        short_percent_of_shares_outstanding char(8),
        shares_short_prior_month char(10),
        forward_annual_dividend_rate decimal(6,4),
        forward_annual_dividend_yield char(8),
        trailing_annual_dividend_rate decimal(6,4),
        trailing_annual_dividend_yield char(8),
        year_average_dividend_yield_5 decimal(6,4),
        payout_ratio char(8),
        dividend_date char(14),
        previous_dividend_date char(14),
        last_stock_split char(12),
        last_split_date char(14),
        fiscal_year_ends char(14),
        recent_quarter char(14),
        profit_margin char(8),
        operating_margin char(8),
        return_assets char(8),
        return_equity char(8),
        revenue char(14),
        revenue_per_share char(14),
        quarterly_revenue_growth char(8),
        gross_profit char(8),
        ebitda char(8),
        net_income_avi_common char(8),
        diluted_eps decimal(6,4),
        quarterly_earnings_growth char(8),
        total_cash char(8),
        total_cash_per_share decimal(6,4),
        total_debt char(8),
        total_debt_over_equity decimal(14,4),
        current_ratio decimal(6,4),
        book_value_per_share decimal(6,4),
        operating_cash_flow char(8),
        levered_free_cash_flow char(8),
        company_id int, 
        FOREIGN KEY (company_id) REFERENCES company_names (id)  
    ); """
    cursor.execute(create_table)


def create_all_historical_table(cursor, table_name='all_historical_data'):

    create_table = f""" 
    CREATE TABLE IF NOT EXISTS {table_name}(
        Id char(32) PRIMARY KEY, 
        Date date,
        Open decimal(18,4),
        High decimal(18,4),
        Low	 decimal(18,4),
        Close decimal(18,4),	
        Volume Bigint,
        Symbol char(6),
        company_id int,
        FOREIGN KEY (company_ID) REFERENCES company_names (id)
    ); """

    cursor.execute(create_table)


# all historical tables to have the following columns
def create_a_historical_table(table_name, cursor):
    """Make sure you have created company_names table before calling this function
     (see function create_company_names_table)"""

    create_table = f""" 
    CREATE TABLE IF NOT EXISTS {table_name}(
--         ID SERIAL PRIMARY KEY,
        Date date PRIMARY KEY,
        Open decimal(18,4),
        High decimal(18,4),
        Low	 decimal(18,4),
        Close decimal(18,4),	
        Volume Bigint,
        Symbol char(6),
        Company_ID int,
        FOREIGN KEY (Company_ID) REFERENCES company_names (id)
    ); """

    cursor.execute(create_table)


# each companies historical
def create_historical_tables(connection):
    """
    looks at the company names table and creates all the individual historical tables
    :param connection:
    :return:
    """
    cursor = connection.cursor()

    rows = pd.read_csv(company_names_file_path)['Symbol'].tolist()
    for i in rows:
        table_name = 't_' + str(i).lower()
        # if '/' in table_name:  # for spacial case BRK/A
        #     table_name = table_name.replace('/', '')
        create_a_historical_table(table_name, cursor)
        connection.commit()
        print(f"Created table {table_name}")
    connection.close()


def drop_a_table(table_name: str, cursor):
    """ make sure to use conn.commit() if dropping tables individually """
    cursor.execute(f"""DROP TABLE IF EXISTS {table_name}""")
    print(f"Dropped table {table_name}")


def truncate_a_table(table_name, cursor):
    cursor.execute(f"""TRUNCATE TABLE {table_name}""")
    print(f"Truncated table {table_name}")


def drop_historical_tables(connection, truncate: bool = False):
    """
    For deletetion of data in tables, other options would be to completely drop the tables, this is one less step
    :param connection:
    :param truncate: if true clean all data but not delete
    :return:
    """
    cursor = connection.cursor()
    file_path = os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                          resources_folder), company_names_file_name)
    rows = pd.read_csv(file_path)['Symbol'].tolist()

    for i in rows:
        table_name = 't_' + str(i).lower()
        # if '/' in table_name:  # for spacial case BRK/A
        #     table_name = table_name.replace('/', '')
        if truncate:
            truncate_a_table(table_name, cursor)
        else:
            drop_a_table(table_name, cursor)
        connection.commit()
    connection.close()


if __name__ == '__main__':
    conn = connect_db()
    curs = conn.cursor()
    # create tables
    # drop tables
    # drop_historical_tables(conn) # make sure to use conn.commit() if dropping tables individually

    # create_all_historical_table(curs)
    # create_all_company_stats_table(curs)
    #
    truncate_a_table('all_company_stats', cursor=curs)
    conn.commit()



