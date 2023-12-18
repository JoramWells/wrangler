import csv
from pony.orm import Database, Optional, db_session, sql_debug
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

df = pd.read_csv('./data/users.csv')

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
     


class Users(db.Entity):
        _table = "Users"
        fullName = Optional(str)
        mobileNumber = Optional(str)
        userName = Optional(str)
        email = Optional(str, default='0')
        userType = Optional(str, default='0')


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
        Users(
            fullName=row["Full Name"],
            mobileNumber=str(row["Mobile Number"]),
            userName=row["User Name"],
            email=str(row["Email"]),
            userType = str(row["User Type"]),
        )

logger.info('Saving to Database...')

save()








    



            
