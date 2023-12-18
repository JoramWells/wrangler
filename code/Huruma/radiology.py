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

df = pd.read_csv('./data/Huruma/Radiology.csv')
# df.drop(columns='No.', inplace=True)

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
     


class Radiology(db.Entity):
        _table = "Radiology"
        serviceCategory = Required(str)
        serviceName = Required(str, default='0')
        serviceCostCash = Required(str, default='0')
        serviceCostCorporate = Required(str, default='0')
        serviceCostInsurance = Required(str, default='0')
        serviceCostForeigner = Required(str, default='0')


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
        Radiology(
            serviceCategory=str(row["Service Category"]),
            serviceName=str(row['Service Name']),
            serviceCostCash=str(row['Service Cost (Cash)']),
            serviceCostCorporate=str(row['Service Cost (Corporate)']),
            serviceCostInsurance=str(row['Service Cost (Insurance)']),
            serviceCostForeigner=str(row['Service Cost (Foreigner)']),

        )

logger.info('Saving to Database...')

save()








    



            
