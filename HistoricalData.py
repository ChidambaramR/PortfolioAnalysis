import datetime
import logging
import sqlite3

import pandas as pd
from nsepy import get_history

from Retry import retry

DB_PATH = "db/historical_data.db"


@retry(tries=5, delay=2, backoff=2)
def get_data(symbol, from_date, to_date):
    logging.info("Fetching historical data from NSEpy for {} from {} till {}".format(symbol, from_date, to_date))
    return get_history(symbol=symbol, start=from_date, end=to_date)


def is_table_exists(symbol):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(symbol)
    is_found = False

    c.execute(query)
    if c.fetchone()[0] == 1:
        logging.info("Table for symbol {} exists".format(symbol))
        is_found = True
    else:
        logging.info("Table for symbol {} does not exist".format(symbol))
        is_found = False

    conn.commit()
    conn.close()

    return is_found


def get_next_start_date(symbol):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM {} ORDER BY ts DESC LIMIT 1".format(symbol)

    df = pd.read_sql_query(query, conn)

    conn.commit()
    conn.close()

    return df['ts'][0]


def get_historical_data_from_db(symbol, start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM '{}' where ts >= '{}' and ts <= '{}'".format(symbol, start_date, end_date)

    df = pd.read_sql_query(query, conn, index_col='ts')
    df.rename(columns={'Close': symbol}, inplace=True)

    if df.empty:
        raise ValueError(
            "Unable to find entries for {} in db for start_date {} and end_date {}".format(symbol, start_date,
                                                                                           end_date))

    conn.commit()
    conn.close()

    return df


def get_historical_data(symbol, start_date):
    end_date = datetime.datetime.now().date()
    """
    conn = sqlite3.connect(DB_PATH)

    if is_table_exists(symbol):
        last_value_date = datetime.datetime.strptime(get_next_start_date(symbol), "%Y-%m-%d").date()
        logging.debug("Date of last close value for {} is {}".format(symbol, last_value_date))

        last_value_date = last_value_date + datetime.timedelta(days=1)
        logging.debug("Querying for start date of {} for {}".format(last_value_date, symbol))

        if last_value_date >= end_date:
            logging.debug("We have upto date data. Probably we are re-running")
            return get_historical_data_from_db(symbol, start_date, end_date)

        new_start_date = last_value_date

        df = get_data(symbol, new_start_date, end_date)

        if not df.empty:
            df.rename(columns={'Date': 'ts'}, inplace=True)
            df.index.names = ['ts']
            df = df[['Close']]
            df.to_sql(symbol, conn, if_exists='append')
    else:
        df = get_data(symbol, start_date, end_date)

        if not df.empty:
            logging.info(
                "No values found for {} from NSEpy with start_date {} and end_date {}".format(symbol, start_date, end_date))
            df.rename(columns={'Date': 'ts'}, inplace=True)
            df.index.names = ['ts']
            df = df[['Close']]
            df.to_sql(symbol, conn, if_exists='fail')

    conn.commit()
    conn.close()
    """

    df = get_historical_data_from_db(symbol, start_date, end_date)
    df.rename(columns={'Close': symbol}, inplace=True)
    return df


def get_historical_data_for_symbols(symbols, start_date):
    df = pd.DataFrame()

    for symbol in symbols:
        _df = get_historical_data(symbol, start_date)
        df = pd.concat([df, _df], axis=1)

    df.index = pd.to_datetime(df.index)
    df = df.fillna(0)
    return df


# get_historical_data_for_symbols(["TCS", "ITC"], datetime.datetime(2022, 10, 1).date())

get_data("TCS", datetime.datetime(2022, 10, 1).date(), datetime.datetime.now().date())
