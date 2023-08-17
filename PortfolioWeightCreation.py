import datetime
import json
import logging

import pandas
import pandas as pd

# Excel column names
from Constants import scrip_map, SCRIP_NAME, QUANTITY, INVESTMENT_VALUE, QUANTITY_PERCENTAGE, \
    INVESTMENT_VALUE_PERCENTAGE, CURRENT_VALUE, CURRENT_VALUE_PERCENTAGE, PRICE, TYPE, BUY_PRICE, SELL_PRICE, SCRIP, \
    NET_PRESENT_VALUE, NET_PRESENT_VALUE_PERCENTAGE, HOLDING
from HistoricalData import get_historical_data_for_symbols
from TradeBookData import get_trade_book
from charts.summary import create_time_series_portfolio_chart


def read_trade_book(file_name):
    def normalise_scrip_name(scrip):
        return scrip_map[scrip]

    df = pandas.read_excel(file_name, sheet_name="Full portfolio", usecols="F:J", header=8, index_col=0)
    df[SCRIP_NAME] = df[SCRIP_NAME].map(normalise_scrip_name)

    symbols_to_skip = ["SGB", "NIPPON LIFE IND"]

    df = df[~df[SCRIP_NAME].isin(symbols_to_skip)]

    return df


def get_percentage(weight_dict):
    total_units = 0
    total_investment_value = 0

    # Find the total holding
    for key, value in weight_dict.items():
        total_units = total_units + value[QUANTITY]
        total_investment_value = total_investment_value + value[INVESTMENT_VALUE]

    for key, value in weight_dict.items():
        value[QUANTITY_PERCENTAGE] = (value[QUANTITY] / total_units) * 100
        value[INVESTMENT_VALUE_PERCENTAGE] = (value[INVESTMENT_VALUE] / total_investment_value) * 100


def update_portfolio_with_stats(portfolio, current_date, closing_prices_df):
    if pd.to_datetime(current_date) not in closing_prices_df.index:
        return portfolio

    portfolio_as_dict = json.loads(portfolio)

    total_current_value = 0
    total_units = 0
    total_investment_value = 0

    for key, value in portfolio_as_dict.items():
        value[CURRENT_VALUE] = value[QUANTITY] * closing_prices_df.loc[pd.to_datetime(current_date)][key]
        total_current_value = total_current_value + value[CURRENT_VALUE]
        total_units = total_units + value[QUANTITY]
        total_investment_value = total_investment_value + value[INVESTMENT_VALUE]

    for key, value in portfolio_as_dict.items():
        value[CURRENT_VALUE_PERCENTAGE] = round((value[CURRENT_VALUE] / total_current_value) * 100, 2)
        value[QUANTITY_PERCENTAGE] = round((value[QUANTITY] / total_units) * 100, 2)
        value[INVESTMENT_VALUE_PERCENTAGE] = round((value[INVESTMENT_VALUE] / total_investment_value) * 100, 2)

    return json.dumps(portfolio_as_dict)


def create_portfolio(symbols, start_date, end_date):
    cols = {col: [] for col in symbols}
    date_list = pd.date_range(start=start_date, end=end_date).tolist()
    df = pd.DataFrame(cols, index=date_list)

    for col in df.columns:
        df[col] = df[col].apply(lambda x: {})

    return df


def construct_portfolio(running_portfolio_json, transactions_df):
    """
    Return the new portfolio weight after including the transactions in the dataframe.

    :param running_portfolio_json: Total portfolio weight so far
    :param transactions_df: Transactions to be included in the portfolio weight
    :return: New portfolio weight
    """

    # Read the portfolio weight so far from JSON
    running_portfolio = json.loads(running_portfolio_json)

    # Iterate the new transactions to be included in the portfolio
    for index, row in transactions_df.iterrows():
        scrip_name = row[SCRIP_NAME]
        quantity = abs(row[QUANTITY])
        price = row[PRICE]

        if scrip_name in running_portfolio:
            if row[TYPE] == "SELL" or row[TYPE] == "S":
                # Reduce the allocation for the given scrip in our portfolio
                running_portfolio[scrip_name][QUANTITY] = running_portfolio[scrip_name][QUANTITY] - quantity

                # Reduce the investment value from portfolio since we have sold
                running_portfolio[scrip_name][INVESTMENT_VALUE] = running_portfolio[scrip_name][INVESTMENT_VALUE] - \
                                                                  (quantity * running_portfolio[scrip_name][BUY_PRICE])

                if running_portfolio[scrip_name][SELL_PRICE] == 0:
                    # If selling for the first time
                    running_portfolio[scrip_name][SELL_PRICE] = price
                else:
                    # Averaging the sell price
                    running_portfolio[scrip_name][SELL_PRICE] = (running_portfolio[scrip_name][SELL_PRICE] + price) / 2
            else:
                # We have bought the scrip for more than once
                running_portfolio[scrip_name][QUANTITY] = running_portfolio[scrip_name][QUANTITY] + quantity

                # Averaging the buy price
                running_portfolio[scrip_name][BUY_PRICE] = (running_portfolio[scrip_name][BUY_PRICE] + price) / 2

                # Add the investment value from portfolio since we have bought
                running_portfolio[scrip_name][INVESTMENT_VALUE] = running_portfolio[scrip_name][INVESTMENT_VALUE] + \
                                                                  (quantity * price)

            if running_portfolio[scrip_name][QUANTITY] == 0:
                # Remove from holding as we have sold everything
                del running_portfolio[scrip_name]
        else:
            # We are buying the scrip for the first time
            running_portfolio[scrip_name] = {
                QUANTITY: quantity,
                QUANTITY_PERCENTAGE: 0.0,
                INVESTMENT_VALUE_PERCENTAGE: 0.0,
                CURRENT_VALUE: price * quantity,
                CURRENT_VALUE_PERCENTAGE: 100.0,
                BUY_PRICE: price,
                SELL_PRICE: 0,
                INVESTMENT_VALUE: price * quantity
            }

    # print(json.dumps(running_portfolio, indent=2))
    # get_percentage(running_portfolio)
    return json.dumps(running_portfolio)


def get_portfolio_df(portfolio):
    portfolio_dict = json.loads(portfolio)
    portfolio_dicts = []

    for key, value in portfolio_dict.items():
        portfolio_dicts.append({
            SCRIP: key,
            QUANTITY: value[QUANTITY],
            QUANTITY_PERCENTAGE: value[QUANTITY_PERCENTAGE],
            INVESTMENT_VALUE: value[INVESTMENT_VALUE],
            INVESTMENT_VALUE_PERCENTAGE: value[INVESTMENT_VALUE_PERCENTAGE],
            CURRENT_VALUE: value[CURRENT_VALUE],
            CURRENT_VALUE_PERCENTAGE: value[CURRENT_VALUE_PERCENTAGE],
            BUY_PRICE: value[BUY_PRICE],
            SELL_PRICE: value[SELL_PRICE]
        })

    df = pd.DataFrame(portfolio_dicts)

    # Add more data here if required
    df[NET_PRESENT_VALUE] = df[CURRENT_VALUE] - df[INVESTMENT_VALUE]
    df[NET_PRESENT_VALUE_PERCENTAGE] = (df[CURRENT_VALUE] - df[INVESTMENT_VALUE]) / df[CURRENT_VALUE]

    return df


def get_portfolio(df, start_date, end_date, closing_prices_df):
    portfolio = {}
    running_portfolio = "{}"

    # One trading day
    delta = datetime.timedelta(days=1)

    # Iterate for all days for which we want to track our portfolio
    while start_date <= end_date:
        if pd.to_datetime(start_date) in df.index:
            logging.info("Found a transaction for date {}".format(start_date))

            # Get portfolio weight until this day
            running_portfolio = construct_portfolio(running_portfolio, df.loc[[start_date]])

        running_portfolio = update_portfolio_with_stats(running_portfolio, start_date, closing_prices_df)

        if running_portfolio == "{}":
            start_date += delta
            continue

        portfolio[start_date] = running_portfolio
        start_date += delta

    portfolio_time_series_df = pd.Series(portfolio).to_frame()
    portfolio_time_series_df.columns = [HOLDING]

    portfolio_df = get_portfolio_df(running_portfolio)
    return portfolio_time_series_df, portfolio_df


def get_npv_for_portfolio_time_series(d):
    if not d:
        return 0
    portfolio_df = get_portfolio_df(d)
    return portfolio_df[NET_PRESENT_VALUE].sum()


def consolidate_portfolio(portfolio_time_series_df):
    portfolio_time_series_df[NET_PRESENT_VALUE] = portfolio_time_series_df[HOLDING].apply(
        lambda x: get_npv_for_portfolio_time_series(x))


def construct_running_portfolio(df, start_date, end_date):
    running_portfolio_units_count = pd.DataFrame()

    # One trading day
    delta = datetime.timedelta(days=1)

    pass


def create_portfolio_weight(file_name, start_date, end_date=datetime.datetime.now().date()):
    # file_name is the name of the Excel file containing the trade book
    logging.info("Starting portfolio analysis from date {} until {}".format(start_date, end_date))

    # Read the trade book Excel file
    # trade_book_df = read_trade_book(file_name)
    trade_book_df = get_trade_book()

    # Get unique scrips held in the timeframe
    symbols = list(set(trade_book_df[SCRIP_NAME].tolist()))

    # Get closing price for all symbols from the trade book
    closing_price_df = get_historical_data_for_symbols(symbols, start_date)

    portfolio = create_portfolio(symbols, start_date, end_date)

    portfolio_time_series_df, portfolio_df = get_portfolio(trade_book_df, start_date, end_date, closing_price_df)
    # print(df)
    portfolio_time_series_df.to_excel("/Users/chidr/Desktop/StockAnalysis/Holding.xlsx",
                                      sheet_name="Holding Time Series")

    # create_latest_portfolio_chart(portfolio_df, end_date.strftime('%Y-%b-%d'))

    consolidate_portfolio(portfolio_time_series_df)
    create_time_series_portfolio_chart(portfolio_time_series_df)
