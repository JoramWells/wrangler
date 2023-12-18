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

df = pd.read_csv('./data/HBPressure/patientsJanFebNoDuplicates.csv')
# df.drop(columns='ID', inplace=True)

# df['REMAINING_QUANTITY'].apply(pd.to_numeric).astype(int)


# df=df['REMAINING_QUANTITY'].astype(int)

df.fillna(0, inplace=True)

# data = csv.DictWriter(file, delimiter=',', fieldnames=headers)


# Database Class
db = Database()

# the different configurations
db_params = dict(provider="mysql", host="localhost",
                 user="root", password="root", db="heart_health")


# if __name__ == "__main__":
     


class Patient(db.Entity):
        _table = "Patient"
        patientName = Required(str)
        idNo = Required(str)
        YOB = Required(str, default='N/A')
        gender = Required(str, default='M')
        mobileNo = Required(str)
        location = Required(str, default='Ichunga')
        date = Required(str, default='0')



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
        Patient(
            patientName=str(row["Name of Client"]),
            idNo=str(row['Unique Client ID']),
            YOB=str(row['YOB']),
            gender=str(row['Gender']),
            mobileNo=str(row['Mobile No.']),
            location=str(row['Location/ Village']),
            date=str(row['Date']),
        )

logger.info('Saving to Database...')

save()








    



            
