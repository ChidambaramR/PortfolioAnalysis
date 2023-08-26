import datetime
import logging

import pandas as pd

# Excel column names
from TradeBookData import get_trade_book, get_broker_transactions
from UpstoxHistoricalData import get_historical_data_for_symbols


# Calculate the percentage difference while handling zeros
def calculate_percentage(row, c1, c2):
    if row[c1] == 0 or row[c2] == 0:
        return 0
    else:
        return round(((row[c2] - row[c1]) / row[c1]) * 100, 2)


def get_portfolio(transactions_df_map, closing_prices_df_map):
    single_stock_portfolios = []

    for symbol, transactions_df in transactions_df_map.items():
        closing_price_df = closing_prices_df_map[symbol]

        if closing_price_df.empty:
            raise ValueError("Cannot have an empty closing price time series")

        if transactions_df.empty:
            raise ValueError("Cannot have an empty transactions data")

        transactions_df.drop('name', axis=1, inplace=True)
        df = pd.merge(closing_price_df, transactions_df, left_index=True, right_index=True, how='outer')
        complete_date_range = pd.date_range(start=df.index.min(), end=df.index.max())
        df = df.reindex(complete_date_range)

        df.fillna(0, inplace=True)
        cum_qty = symbol +'-cum_qty'
        cum_money_in_stock = symbol + '-cum_money_in_stock'
        net_amount = symbol + '-net_amount'
        pv = symbol + '-pv'

        df['money_in_stock'] = -df['net_amount']
        df[cum_qty] = df['net_qty'].cumsum()
        df[cum_money_in_stock] = df['money_in_stock'].cumsum()
        df.loc[df[cum_qty] == 0, cum_money_in_stock] = 0
        df[net_amount] = df['net_amount']
        df[pv] = df[cum_qty] * df['Close']
        df[symbol] = df.apply(calculate_percentage, args=(cum_money_in_stock, pv), axis=1)


        df = df[[cum_qty, cum_money_in_stock, net_amount, pv]]
        single_stock_portfolios.append(df)

    portfolio = pd.concat(single_stock_portfolios, axis=1)
    portfolio.fillna(0, inplace=True)
    portfolio['money_in_stock'] = 0
    portfolio['value'] = 0
    portfolio['cash_from_stock'] = 0

    for symbol in transactions_df_map.keys():
        portfolio['money_in_stock'] = portfolio['money_in_stock'] + portfolio[symbol + '-cum_money_in_stock']
        portfolio['value'] = portfolio['value'] + portfolio[symbol + '-pv']
        portfolio['cash_from_stock'] = portfolio['cash_from_stock'] + portfolio[symbol + '-net_amount']

    return portfolio


def create_portfolio(from_date, to_date=datetime.datetime.now().date()):
    logging.basicConfig(level=logging.INFO)
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

    #portfolio['cash_invested'] = portfolio['cash_invested'].cumsum()
    # portfolio['brokerage'] = portfolio['brokerage'].cumsum()
    #portfolio['net_cash'] = 0
    portfolio['cash_deployed'] = portfolio['cash_from_stock'].cumsum()
    portfolio['total_cash_invested'] = portfolio['cash_invested'].cumsum()
    portfolio['total_brokerage'] = portfolio['brokerage'].cumsum()

    portfolio['net_cash'] = portfolio['cash_from_stock'] + portfolio['cash_invested'] + portfolio['brokerage']
    portfolio['trading_profit'] = (portfolio['cash_from_stock'] + portfolio['brokerage']).cumsum()
    portfolio['running_balance'] = portfolio['net_cash'].cumsum()
    portfolio['liquid_value'] = portfolio['value'] + portfolio['running_balance']
    portfolio['net_profit'] = portfolio['value'] + portfolio['trading_profit']
    portfolio.index = (portfolio.index).date

    summary = portfolio[['total_cash_invested', 'liquid_value', 'net_profit']]
    individual_stock_returns = portfolio[symbols_to_isin_dict.keys()]
    stock_investment_performance = portfolio[['money_in_stock', 'value']]

    return summary, individual_stock_returns, stock_investment_performance

