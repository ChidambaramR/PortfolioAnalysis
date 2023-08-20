import datetime
import itertools
import json
import logging

import pandas as pd
from sqlalchemy import create_engine

from Constants import SCRIP_NAME, TYPE, QUANTITY, PRICE
from SymbolFetcher import fetch_symbols

# Write code to read a file from disk and return the contents as a json array
def read_file(file_path):
    with open(file_path) as f:
        return json.load(f)

'''
$(document).ready(function() {
  var table = $('#myTable').DataTable();

  // Disable pagination
  table.page.len(-1).draw();
});
'''


def get_trade_book():
    def get_symbol_details():
        file_path = 'data/stock_codes.db'
        database_url = f'sqlite:///{file_path}'
        engine = create_engine(database_url)
        sql_table_name = "codes"
        sql_query = f"SELECT code, name, isin FROM {sql_table_name}"
        sql_df = pd.read_sql_query(sql_query, con=engine)
        return sql_df

    mse_trade_book = pd.DataFrame(read_file('/Users/chidr/Desktop/Workdocs/PortfolioAnalysis/tradebook/market_transactions.json'))
    mse_trade_book['transaction_date'] = pd.to_datetime(mse_trade_book['transaction_date'], format='%d/%m/%Y').dt.date
    mse_trade_book.rename(columns={"scrip_code": "code", "scrip_name": "name", "transaction_date": "ts"}, inplace=True)

    symbols_to_skip = ["SGB", "NIPPON LIFE IND", "TOC BSE EXCHANG", "TOC NSE EXCHANG", "STT", "STAMP DUTY", "SERVICE TAX", "TURNOVER CHARGE", "BALLARPUR"]
    mse_trade_book = mse_trade_book[~mse_trade_book['name'].isin(symbols_to_skip)]
    mse_trade_book = mse_trade_book[mse_trade_book['code'] != "0000"]
    mse_trade_book = mse_trade_book[~((mse_trade_book['buy_qty'] != 0) & (mse_trade_book['sell_qty'] != 0))]

    '''
    mse_trade_book[TYPE] = mse_trade_book.apply(lambda row: 'B' if row['buy_qty'] > 0 else 'S', axis=1)
    mse_trade_book[QUANTITY] = mse_trade_book.apply(
        lambda row: row['net_qty'] if row['net_qty'] > 0 else row['net_qty'] * -1, axis=1)
    mse_trade_book[PRICE] = mse_trade_book.apply(
        lambda row: row['net_amount'] if row['net_amount'] > 0 else row['net_amount'] * -1, axis=1)
    '''

    fetch_symbols(mse_trade_book['code'])
    symbols_df = get_symbol_details()

    mse_trade_book['name'] = mse_trade_book['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['name'].iloc[0])
    mse_trade_book['isin'] = mse_trade_book['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['isin'].iloc[0])

    symbols_to_isin_dict = dict(zip(mse_trade_book['name'], mse_trade_book['isin']))
    mse_trade_book = mse_trade_book[['ts', 'name', 'net_qty', 'net_amount']]
    mse_trade_book = mse_trade_book.groupby(['ts', 'name'])['net_qty', 'net_amount'].sum().reset_index()
    mse_trade_book = mse_trade_book.sort_values('ts', ascending=True)
    mse_trade_book.set_index('ts', inplace=True)

    return mse_trade_book, symbols_to_isin_dict


def get_broker_transactions():
    def calculate_cash_invested(row):
        if row['transaction_type'] == 'PAYMENT':
            return -row['debit']
        elif row['transaction_type'] == 'RECEIPT':
            return row['credit']
        else:
            return 0

    def calculate_brokerage(row):
        if row['transaction_type'] == 'JV':
            if row['debit'] != 0:
                return -row['debit']
            elif row['credit'] != 0:
                logging.warn("Credit value for JV {}".format(row['credit']))
                return row['credit']
        else:
            return 0

    broker_transactions = pd.DataFrame(read_file('/Users/chidr/Desktop/Workdocs/PortfolioAnalysis/tradebook/all_transactions.json'))
    broker_transactions['transaction_date'] = pd.to_datetime(broker_transactions['transaction_date'], format='%d/%m/%Y').dt.date

    broker_transactions['cash_invested'] = broker_transactions.apply(calculate_cash_invested, axis=1)
    broker_transactions['brokerage'] = broker_transactions.apply(calculate_brokerage, axis=1)

    broker_transactions.rename(columns={"transaction_date": "ts"}, inplace=True)

    broker_transactions = broker_transactions[['ts', 'cash_invested', 'brokerage']]
    broker_transactions = broker_transactions.groupby(['ts'])['cash_invested', 'brokerage'].sum().reset_index()
    #broker_transactions['cash_invested'] = broker_transactions['cash_invested'].cumsum()
    #broker_transactions['brokerage'] = broker_transactions['brokerage'].cumsum()
    broker_transactions = broker_transactions.sort_values('ts', ascending=True)
    broker_transactions.set_index('ts', inplace=True)
    broker_transactions = broker_transactions.sort_index()
    return broker_transactions

'''
    mse_trade_book.append([
        {
            'transaction_date': datetime.date(2020, 9, 5),
            'code': "800327",
            'name': "SGB",
            'buy_qty': 2,
            'buy_rate': 5067,
            'buy_amount': 10134,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 2,
            'net_rate': 5067,
            'net_amount': -10134
        },
        {
            'transaction_date': datetime.date(2020, 10, 14),
            'code': "800328",
            'name': "SGB",
            'buy_qty': 2,
            'buy_rate': 5001,
            'buy_amount': 10002,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 2,
            'net_rate': 5001,
            'net_amount': -10002
        },
        {
            'transaction_date': datetime.date(2020, 11, 11),
            'code': "800329",
            'name': "SGB",
            'buy_qty': 2,
            'buy_rate': 5127,
            'buy_amount': 10254,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 2,
            'net_rate': 5127,
            'net_amount': -10254
        },
        {
            'transaction_date': datetime.date(2020, 12, 30),
            'code': "800331",
            'name': "SGB",
            'buy_qty': 3,
            'buy_rate': 4950,
            'buy_amount': 14850,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 3,
            'net_rate': 4950,
            'net_amount': -14850
        },
        {
            'transaction_date': datetime.date(2021, 1, 13),
            'code': "800332",
            'name': "SGB",
            'buy_qty': 3,
            'buy_rate': 5054,
            'buy_amount': 15162,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 3,
            'net_rate': 5054,
            'net_amount': -15162
        },
        {
            'transaction_date': datetime.date(2021, 2, 3),
            'code': "800332",
            'name': "SGB",
            'buy_qty': 3,
            'buy_rate': 5054,
            'buy_amount': 15162,
            'sell_qty': 0,
            'sell_rate': 0,
            'sell_amount': 0,
            'net_qty': 3,
            'net_rate': 5054,
            'net_amount': -15162
        }
    ])
'''

get_broker_transactions()