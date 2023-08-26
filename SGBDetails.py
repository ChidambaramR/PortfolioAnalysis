import os
import re

import fitz
import pandas as pd


def read_ticket_from_file(name):
    doc = fitz.Document(name)
    return doc

def get_sgb_details():
    dir_name = '/Users/chidr/Desktop/MyDocuments/Investments/India/SGB Details'
    sgb_transactions = []

    for filename in os.listdir(dir_name):
        if os.path.isfile(os.path.join(dir_name, filename)):
            file_path = os.path.join(dir_name, filename)
            print("Processing File {}".format(file_path))
            doc = read_ticket_from_file(file_path)

            for page in doc:
                words_in_page = page.get_text("words_in_page", sort=True).split("\n")
                if "20140686" in words_in_page:
                    continue

                buy_qty = int(words_in_page[38])
                buy_rate = float(words_in_page[39])
                buy_amount = float(words_in_page[44].replace(',', ''))
                name = words_in_page[15]
                words = name.split()
                name = '-'.join(words).upper()

                sgb_transactions.append({
                    "ts": words_in_page[0],
                    "code": words_in_page[13],
                    "isin": words_in_page[13],
                    "buy_qty": buy_qty,
                    "buy_rate": buy_rate,
                    "buy_amount": buy_amount,
                    "sell_qty": 0,
                    "sell_rate": 0,
                    "sell_amount": 0,
                    "net_qty": buy_qty,
                    "net_rate": buy_rate,
                    "net_amount": -buy_amount,
                    "name": name
                })

    sgb_df = pd.DataFrame(sgb_transactions)
    sgb_df['ts'] = pd.to_datetime(sgb_df['ts']).dt.date
    return sgb_df



