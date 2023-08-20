import datetime
import logging

from PortfolioWeightCreation import create_portfolio_weight

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', level=logging.INFO)
    create_portfolio_weight(datetime.datetime(2019, 6, 1).date())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
