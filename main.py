import datetime
import logging

from PortfolioWeightCreation import create_portfolio
from charts.chart import plot_summary

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', level=logging.INFO)
    df = create_portfolio(datetime.datetime(2019, 6, 1).date())
    plot_summary(df)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
