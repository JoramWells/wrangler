import csv
from pony.orm import Database, Required, db_session, sql_debug
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_csv(csv_file):

    # read csv file
    file = open(csv_file,'r')
    df = csv.DictReader(file)
    return df

df = pd.read_csv('./data/Huruma/Items.csv')
df.drop(columns='No.', inplace=True)

# df['REMAINING_QUANTITY'].apply(pd.to_numeric).astype(int)


# df=df['REMAINING_QUANTITY'].astype(int)

df.fillna(0, inplace=True)

# data = csv.DictWriter(file, delimiter=',', fieldnames=headers)


# Database Class
db = Database()

# the different configurations
db_params = dict(provider="mysql", host="localhost",
                 user="root", password="root", db="huruma")


# if __name__ == "__main__":
     


class Item(db.Entity):
        _table = "Item"
        itemCode = Required(str)
        itemDescription = Required(str, default='0')
        itemCategory = Required(str, default='0')
        brand = Required(str, default='0')
        uom = Required(str, default='0')
        buyingPrice = Required(str, default='0')
        sellingPrice = Required(str, default='0')
        expenseAccount = Required(str, default='0')
        incomeAccount = Required(str, default='0')




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
        Item(
            itemCode=str(row["Item Code"]),
            itemDescription=str(row['Item Description']),
            itemCategory=str(row['Item Category']),
            brand=str(row['Brand']),
            uom=str(row['U.O.M']),
            buyingPrice=str(row['Buying Price']),
            sellingPrice=str(row['Selling Price']),
            expenseAccount=str(row['Expense Account']),
            incomeAccount=str(row['Income Account']),


        )

logger.info('Saving to Database...')

save()








    



            
