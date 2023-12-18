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

df = pd.read_csv('./data/HURUMA HOSPITAL JANUARY 2023 B.P SCREENING DATA.csv')
df.drop(columns='ID', inplace=True)

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
     


class ScreeningData(db.Entity):
        _table = "ScreeningData"
        patientName = Required(str)
        patientID = Required(str)
        YOB = Required(str)
        gender = Required(str, default='0')
        mobileNo = Required(str, default='0')
        location = Required(str, default='0')
        isNew = Required(str, default='0')
        knownHTN = Required(str, default='0')
        isTakingMeds = Required(str, default='0')
        BP = Required(str, default='0')
        normal = Required(str, default='0')
        highBP = Required(str, default='0')
        referred = Required(str, default='0')
        referralNo = Required(str, default='0')
        facilityReferred = Required(str, default='0')
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
        ScreeningData(
            patientName=str(row["Name of Client"]),
            patientID=str(row["Unique Client ID"]),
            YOB=str(row["YOB"]),
            gender=str(row["Gender"]),
            mobileNo = str(row["Mobile No."]),
            location=str(row["Location/ Village"]),
            isNew=str(row['New']),
            knownHTN=str(row['Known HTN']),
            isTakingMeds=str(row['Taking meds']),
            BP=str(row['B.P']),
            normal=str(row['Normal']),
            highBP=str(row['High B.P']),
            referred=str(row['Referred']),
            referralNo=str(row['Referral No.']),
            facilityReferred=str(row['Facility referred']),
            date=str(row['Date']),


        )

logger.info('Saving to Database...')

save()








    



            
