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

df = pd.read_csv('./data/ward.csv')

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
     


class Wards(db.Entity):
        _table = "Wards"
        branch = Optional(str)
        wardType = Optional(str)
        wardDescription = Optional(str)
        admissionChargeNonCorporate = Optional(str, default='0')
        admissionChargeCorporate = Optional(str, default='0')
        dailyRateNonCorporate = Optional(str, default='0')
        dailyRateCorporate = Optional(str, default='0')
        nursingDailyChargeNonCorporate = Optional(str)
        nursingDailyChargeCorporate = Optional(str)


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
        Wards(
            branch=row["Branch"],
            wardType=row["Ward Type"],
            wardDescription=row["Ward Description"],
            admissionChargeNonCorporate=str(row["Admission Charge(Non-Corporate)"]),
            admissionChargeCorporate = str(row["Admission Charge(Corporate)"]),
            dailyRateNonCorporate=str(row["Daily Rate(Non-Corporate)"]),
            dailyRateCorporate=str(row['Daily Rate(Corporate)']),
            nursingDailyChargeNonCorporate=str(row['Nursing Daily Charge(Non-Corporate)']),
            nursingDailyChargeCorporate=str(row['Nursing Daily Charge(Corporate)']),

        )

logger.info('Saving to Database...')

save()








    



            
