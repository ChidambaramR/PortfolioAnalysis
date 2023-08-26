import logging
import sqlite3
from time import sleep

import pandas as pd
import upstox_client
import datetime

from upstox_client.rest import ApiException

DB_PATH = "db/historical_data.db"


def is_table_exists(symbol):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(symbol)
    is_found = False

    c.execute(query)
    if c.fetchone()[0] == 1:
        logging.debug("Table for symbol {} exists".format(symbol))
        is_found = True
    else:
        logging.warn("Table for symbol {} does not exist".format(symbol))
        is_found = False

    conn.commit()
    conn.close()

    return is_found


def get_next_start_date(symbol):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM '{}' ORDER BY ts DESC LIMIT 1".format(symbol)

    df = pd.read_sql_query(query, conn)

    conn.commit()
    conn.close()

    return df['ts'][0]


def get_historical_data_from_db(symbol, from_date, to_date):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM '{}' where ts >= '{}' and ts <= '{}'".format(symbol, from_date, to_date)

    df = pd.read_sql_query(query, conn, index_col='ts')
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
    df.sort_values('ts', ascending=True)

    complete_date_range = pd.date_range(start=df.index.min(), end=df.index.max())
    df = df.reindex(complete_date_range)
    df = df.ffill()

    if df.empty:
        raise ValueError(
            "Unable to find entries for {} in db for from_date {} and to_date {}".format(symbol, from_date,
                                                                                           to_date))

    conn.commit()
    conn.close()

    return df


def get_data(api_instance, instrument_key, instrument_name, from_date, to_date):
    instrument_key = 'NSE_EQ|' + instrument_key
    interval = 'day'  # str |
    api_version = '2.0'  # str | API Version Header
    data_df = pd.DataFrame()

    while from_date <= to_date:
        current_year_to_date = min(to_date,
                                   from_date.replace(year=from_date.year + 1, month=1, day=1) - datetime.timedelta(
                                       days=1))
        try:
            # Historical candle data
            logging.info("Querying upstox for symbol {} / isin {} for from_date {} till to_date {}".format(instrument_name, instrument_key, from_date, current_year_to_date))
            api_response = api_instance.get_historical_candle_data1(instrument_key, interval, current_year_to_date, from_date,
                                                                    api_version).to_dict()
            column_names = ['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'NA']
            df = pd.DataFrame(api_response['data']['candles'], columns=column_names)
            df['ts'] = pd.to_datetime(df['ts']).dt.date
            data_df = pd.concat([data_df, df], ignore_index=True)

            from_date = current_year_to_date + datetime.timedelta(days=1)
            sleep(5)
        except ApiException as e:
            raise ValueError("Exception when calling HistoryApi->get_historical_candle_data1: %s\n" % e)

    data_df = data_df[['ts', 'Close']]
    data_df.set_index('ts', inplace=True)
    return data_df

def get_historical_data(symbol, isin, from_date, to_date = datetime.datetime.now().date()):
    api_instance = upstox_client.HistoryApi()

    conn = sqlite3.connect(DB_PATH)

    if is_table_exists(symbol):
        last_value_date = datetime.datetime.strptime(get_next_start_date(symbol), "%Y-%m-%d").date()
        logging.debug("Date of last close value for {} is {}".format(symbol, last_value_date))

        last_value_date = last_value_date + datetime.timedelta(days=1)
        logging.debug("Querying for start date of {} for {}".format(last_value_date, symbol))

        if last_value_date >= to_date:
            logging.debug("We have upto date data. Probably we are re-running")
            return get_historical_data_from_db(symbol, from_date, to_date)

        new_start_date = last_value_date

        df = get_data(api_instance, isin, symbol, new_start_date, to_date)

        if not df.empty:
            df.to_sql(symbol, conn, if_exists='append', index=True)
    else:
        df = get_data(api_instance, isin, symbol, from_date, to_date)

        if not df.empty:
            logging.info(
                "Inserting new entry for {} from Upstox with from_date {} and to_date {}".format(symbol, from_date, to_date))
            df.to_sql(symbol, conn, if_exists='fail', index=True)
        else:
            logging.error("No value found for {} from Upstox with from_date {} and to_date {}".format(symbol, from_date, to_date))

    conn.commit()
    conn.close()

    df = get_historical_data_from_db(symbol, from_date, to_date)
    return df

def get_historical_data_for_symbols(symbols_map, from_date, end_date = datetime.datetime.now().date()):
    closing_prices_df_map = {}

    for symbol, isin in symbols_map.items():
        closing_prices_df_map[symbol] = get_historical_data(symbol, isin, from_date, end_date)

    return closing_prices_df_map

# get_historical_data('ITC','INE154A01025', datetime.date(2023, 2, 20), datetime.date(2023, 2, 28))