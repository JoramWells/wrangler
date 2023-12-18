import csv
from pony.orm import *
import pandas as pd
import logging


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_csv(csv_file):

    # read csv file
    file = open(csv_file,'r')
    df = csv.DictReader(file)
    return df

df = pd.read_csv('./price_list_items.csv')

# data = csv.DictWriter(file, delimiter=',', fieldnames=headers)


# Database Class
db = Database()

# the different configurations
db_params = dict(provider="mysql", host="localhost",
                 user="root", password="root", db="huruma")


# if __name__ == "__main__":
     


class PriceListItems(db.Entity):
        _table = "PriceListItems"
        item_code = Optional(str)
        item_description = Optional(str)
        item_category = Optional(str)
        uom = Optional(str)
        buying_price = Optional(str)
        selling_price = Optional(str)
        expense_account = Optional(str)
        income_account = Optional(str)

sql_debug(True)

# # bind the different attributes
db.bind(**db_params)
db.generate_mapping(create_tables=True)  # Create tables


# ================================= Saving to the DB =========================

data =[]
for _, v in df.items():
    data.append(v)


print((df))

@db_session
def save():
    for idx, row in df.iterrows():
        PriceListItems(
            item_code=row["Item Code"],
            item_description=row["Item Description"],
            item_category=row["Item Category"],
            uom=str(row["U.O.M"]),
            buying_price = str(row["Buying Price"]),
            selling_price=str(row["Selling Price"]),
            expense_account=str(row['Expense Account']),
            income_account = str(row['Income Account'])
        )

logger.info('Saving to Database...')

save()








    



            
