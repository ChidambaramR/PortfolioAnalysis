import datetime
import logging

import pandas as pd

# Excel column names
from TradeBookData import get_trade_book, get_broker_transactions
from UpstoxHistoricalData import get_historical_data_for_symbols


def get_portfolio(transactions_df_map, closing_prices_df_map):
    single_stock_portfolios = []

    for symbol, transactions_df in transactions_df_map.items():
        closing_price_df = closing_prices_df_map[symbol]

        if closing_price_df.empty:
            raise ValueError("Cannot have an empty closing price time series")

        if transactions_df.empty:
            raise ValueError("Cannot have an empty transactions data")

        transactions_df.drop('name', axis=1, inplace=True)
        df = pd.merge(closing_price_df, transactions_df, left_index=True, right_index=True, how='left')
        df.fillna(0, inplace=True)
        cum_qty = symbol +'-cum_qty'
        cum_money_invested = symbol + '-cum_money_invested'
        cash_in_hand = symbol + '-cih'
        pv = symbol + '-pv'

        df['money_invested'] = -df['net_amount']
        df[cum_qty] = df['net_qty'].cumsum()
        df[cum_money_invested] = df['money_invested'].cumsum()
        df.loc[df[cum_qty] == 0, cum_money_invested] = 0
        df[cash_in_hand] = df['net_amount'].cumsum()
        df[pv] = df[cum_qty] * df['Close']

        df = df[[cum_qty, cum_money_invested, cash_in_hand, pv]]
        single_stock_portfolios.append(df)

    portfolio = pd.concat(single_stock_portfolios, axis=1)
    portfolio.fillna(0, inplace=True)
    portfolio['money_invested'] = 0
    portfolio['value'] = 0
    portfolio['cash_in_hand'] = 0

    for symbol in transactions_df_map.keys():
        portfolio['money_invested'] = portfolio['money_invested'] + portfolio[symbol + '-cum_money_invested']
        portfolio['value'] = portfolio['value'] + portfolio[symbol + '-pv']
        portfolio['cash_in_hand'] = portfolio['cash_in_hand'] + portfolio[symbol + '-cih']

    return portfolio


def create_portfolio_weight(from_date, to_date=datetime.datetime.now().date()):
    logging.info("Starting portfolio analysis from date {} until {}".format(from_date, to_date))

    trade_book_df, symbols_to_isin_dict = get_trade_book()

    # Get closing price for all symbols from the trade book
    closing_price_by_symbols_map = get_historical_data_for_symbols(symbols_to_isin_dict, from_date, to_date)
    grouped_transactions = trade_book_df.groupby('name')
    transactions_by_symbols_map = {name: group for name, group in grouped_transactions}

    pv = get_portfolio(transactions_by_symbols_map, closing_price_by_symbols_map)

    broker_transactions = get_broker_transactions()
    portfolio = pd.merge(pv, broker_transactions, left_index=True, right_index=True, how='left')
    portfolio.fillna(0, inplace=True)

    portfolio['cash_invested'] = portfolio['cash_invested'].cumsum()
    portfolio['brokerage'] = portfolio['brokerage'].cumsum()
    portfolio['net_cash'] = portfolio['cash_in_hand'] + portfolio['cash_invested'] + portfolio['brokerage']

    print(portfolio.head())

