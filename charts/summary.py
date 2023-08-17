from babel.numbers import format_currency
from matplotlib import pyplot as plt

from Constants import SCRIP, QUANTITY, INVESTMENT_VALUE, CURRENT_VALUE, NET_PRESENT_VALUE_PERCENTAGE, \
    NET_PRESENT_VALUE


def plot_summary_bar_chart(x, y, x_label, y_label, title, colors, footnote):
    fig, ax = plt.subplots(figsize=(16, 9))
    ax.barh(x, y, color=colors)
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    for p in ax.patches:
        ax.text(p.get_width() + 0.1, p.get_y() + 0.25,
                str(round((p.get_width()), 2)),
                fontsize=8, fontweight='bold',
                color='grey')

    plt.figtext(0.5, 0.02, footnote, ha="center", fontsize=10,
                bbox={"facecolor": "orange", "alpha": 0.5, "pad": 5})
    plt.savefig("/Users/chidr/Desktop/StockAnalysis/{}".format(title))


def plot_npv_pct_bar_chart(x, y, x_label, y_label, title, colors):
    fig, ax = plt.subplots(figsize=(16, 9))
    ax.barh(x, y, color=colors)
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    for p in ax.patches:
        ax.text(p.get_width() + 0.001, p.get_y() + 0.25,
                str(round(p.get_width() * 100, 2)) + "%",
                fontsize=8, fontweight='bold',
                color='grey')


def create_latest_portfolio_chart(portfolio_df, end_date):
    scrips = portfolio_df[SCRIP].tolist()
    scrip_counts = portfolio_df[QUANTITY].tolist()
    investment_value = portfolio_df[INVESTMENT_VALUE].tolist()
    current_value = portfolio_df[CURRENT_VALUE].tolist()
    pl = portfolio_df[NET_PRESENT_VALUE_PERCENTAGE].tolist()

    title = "No of shares held as of {}".format(end_date)
    footnote = "Total shares: {}".format(portfolio_df[QUANTITY].sum())
    plot_summary_bar_chart(scrips, scrip_counts, "No of shares", "Scrip", title, None, footnote)

    title = "Investment value of shares as of {}".format(end_date)
    footnote = "Total investment value: {}".format(
        format_currency(portfolio_df[INVESTMENT_VALUE].sum(), 'INR', locale='en_IN'))
    plot_summary_bar_chart(scrips, investment_value, "Investment Value", "Scrip", title, None, footnote)

    title = "Current value of shares as of {}".format(end_date)
    colors = list(map(lambda npv: "green" if npv > 0 else "red", portfolio_df[NET_PRESENT_VALUE].tolist()))
    footnote = "Total current value: {}".format(
        format_currency(portfolio_df[CURRENT_VALUE].sum(), 'INR', locale='en_IN'))
    plot_summary_bar_chart(scrips, current_value, "Current Value", "Scrip", title, colors, footnote)
    plt.savefig("/Users/chidr/Desktop/StockAnalysis/{}".format(title))

    title = "PL as of {}".format(end_date)
    plot_npv_pct_bar_chart(scrips, pl, "Current Value", "Scrip", title, colors)
    plt.savefig("/Users/chidr/Desktop/StockAnalysis/{}".format(title))


def create_time_series_portfolio_chart(portfolio_time_series_df):
    #    fig, ax = plt.plot
    portfolio_time_series_df[[NET_PRESENT_VALUE]].plot()
    plt.show()
