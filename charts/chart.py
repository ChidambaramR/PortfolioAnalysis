from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import mplcursors

# Format y-axis tick labels as Indian Rupees in lakhs
def lakhs_formatter(x, pos):
    return f'{int(x/100000):,} Lac'


def plot_summary(df):
    """
    buys = df[df['Action'] == 'Buy']
    sells = df[df['Action'] == 'Sell']

    fig, ax = plt.subplots()
    ax.plot(df['Date'], df['Price'], label='Price')

    buys_circles = ax.scatter(buys['Date'], buys['Price'], c='blue', label='Buy', s=50, alpha=0.7)
    sells_circles = ax.scatter(sells['Date'], sells['Price'], c='grey', label='Sell', s=50, alpha=0.7)
    """
    fig, axs = plt.subplots(1, 2, figsize=(10, 16))
    formatter = mticker.FuncFormatter(lakhs_formatter)

    axs[0].plot(df.index, df['total_cash_invested'], label='Cash Invested')
    axs[0].plot(df.index, df['liquid_value'], label='Portfolio Value')
    axs[0].plot(df.index, df['net_profit'], label='Net Profit')
    axs[0].set_title('Cash Invested vs Portfolio Value Vs Profit')
    axs[0].yaxis.set_major_formatter(formatter)
    mplcursors.cursor(axs[0]).connect(
        "add", lambda sel: sel.annotation.set_text(f"{lakhs_formatter(sel.target[1], None)}"))
    axs[0].legend()

    axs[1].plot(df.index, df['money_in_stock'], label='Stock investement')
    axs[1].plot(df.index, df['value'], label='Present Value')
    axs[1].set_title('Stock Investment vs Present Value')
    axs[1].yaxis.set_major_formatter(formatter)
    mplcursors.cursor(axs[1]).connect(
        "add", lambda sel: sel.annotation.set_text(f"{lakhs_formatter(sel.target[1], None)}"))
    axs[1].legend()

    plt.tight_layout()
    plt.show()