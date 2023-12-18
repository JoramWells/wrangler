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

df = pd.read_csv('./data/Huruma/CurentlyAdmitted.csv')
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
     


class Patients(db.Entity):
        _table = "Patients"
        inPatientFileNo = Required(str)
        fullName = Required(str, default='0')
        age = Required(str, default='0')
        admDate = Required(str, default='0')
        admTime = Required(str, default='0')
        paymentDetails = Required(str, default='0')
        ward = Required(str, default='0')
        bedNo = Required(str, default='0')
        noOfAdmittedDays = Required(str, default='0')
        isCurrent = Required(bool,default=1)


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
        Patients(
            inPatientFileNo=str(row["In-Patient File No"]),
            fullName=str(row['Full Name']),
            age=str(row['Age']),
            admDate=str(row['Admission Date']),
            admTime=str(row['Admission Time']),
            paymentDetails=str(row['Payment Details']),
            ward=str(row['Ward']),
            bedNo=str(row['Bed Number']),
            noOfAdmittedDays=str(row['No of Days Admitted']),

        )

logger.info('Saving to Database...')

save()








    



            
