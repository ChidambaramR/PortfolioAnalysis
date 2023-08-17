import datetime
import itertools
import json

import pandas as pd
from sqlalchemy import create_engine

from Constants import SCRIP_NAME, TYPE, QUANTITY, PRICE
from SymbolFetcher import fetch_symbols

mse_trade_book = pd.DataFrame()
scrip_map_df = pd.read_excel('data/EQUITY_L.xlsx')


# Write code to read a file from disk and return the contents as a json array
def read_file(file_path):
    with open(file_path) as f:
        return json.load(f)


# Write code to create a pandas dataframe from the json array
def create_dataframe(json_array):
    return pd.DataFrame(json_array)


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

    global mse_trade_book
    mse_trade_book = create_dataframe(read_file('/Users/chidr/Desktop/Workdocs/PortfolioAnalysis/tradebook/market_transactions.json'))
    mse_trade_book['transaction_date'] = pd.to_datetime(mse_trade_book['transaction_date'], format='%d/%m/%Y').dt.date
    mse_trade_book.rename(columns={"scrip_code": "code", "scrip_name": "name"}, inplace=True)

    mse_trade_book = mse_trade_book.sort_values('transaction_date', ascending=False)

    symbols_to_skip = ["SGB", "NIPPON LIFE IND", "TOC BSE EXCHANG", "TOC NSE EXCHANG", "STT", "STAMP DUTY", "SERVICE TAX", "TURNOVER CHARGE"]
    mse_trade_book = mse_trade_book[~mse_trade_book['name'].isin(symbols_to_skip)]

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
        }
    ])
    fetch_symbols(mse_trade_book['code'])
    symbols_df = get_symbol_details()

    mse_trade_book['name'] = mse_trade_book['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['name'].iloc[0])
    mse_trade_book['isin'] = mse_trade_book['code'].map(lambda x: symbols_df[symbols_df['code'] == x]['isin'].iloc[0])


def get_transactions():
    get_trade_book()
    transaction_df = mse_trade_book[mse_trade_book['code'] != "0000"]
    transaction_df = transaction_df[~((transaction_df['buy_qty'] != 0) & (transaction_df['sell_qty'] != 0))]
    transaction_df[TYPE] = transaction_df.apply(lambda row: 'B' if row['buy_qty'] > 0 else 'S', axis=1)
    transaction_df[QUANTITY] = transaction_df.apply(
        lambda row: row['net_qty'] if row['net_qty'] > 0 else row['net_qty'] * -1, axis=1)
    transaction_df[PRICE] = transaction_df.apply(
        lambda row: row['net_amount'] if row['net_amount'] > 0 else row['net_amount'] * -1, axis=1)

    # sell_tx_df = transaction_df[transaction_df[TYPE] == 'S']

    # file_path = '/Users/chidr/Downloads/msefsl_tx.db'
    # database_url = f'sqlite:///{file_path}'
    # engine = create_engine(database_url)
    # transaction_df.to_sql('TX', con=engine, if_exists='replace')

    print(transaction_df.head())


def get_combinations():
    key_features = ["key_item_package_quantity",
                    "key_item_height", "key_item_width", "key_item_length", "key_item_weight",
                    "key_pkg_height", "key_pkg_width", "key_pkg_length", "key_pkg_weight", "key_Product Group Code",
                    "key_item_volume", "key_pkg_volume"]
    cand_features = ["cand_item_package_quantity",
                     "cand_item_height", "cand_item_width", "cand_item_length", "cand_item_weight",
                     "cand_pkg_height", "cand_pkg_width", "cand_pkg_length", "cand_pkg_weight",
                     "cand_Product Group Code",
                     "cand_item_volume", "cand_pkg_volume"]
    all_combinations = list(itertools.permutations(key_features + cand_features, 3))
    print(len(all_combinations))
    unique_combinations = [list(combo) for combo in set(all_combinations)]
    print(len(unique_combinations))

    i = 0
    my_combinations = []
    while i < len(key_features):
        j = i + 1
        while j < len(key_features):
            k = 0
            while k < len(cand_features):
                my_combinations.append([key_features[i], key_features[j], cand_features[k]])
                k = k + 1
            j = j + 1
        i = i + 1
    print(my_combinations)
    print(len(my_combinations))


    #print(unique_combinations)


#get_transactions()
get_combinations()
