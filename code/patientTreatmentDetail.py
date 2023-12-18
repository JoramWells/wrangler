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

df = pd.read_csv('./data/patientTreatmentDetail.csv')
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
     


class Treatment(db.Entity):
        _table = "Treatment"
        patientName = Required(str)
        systolicBP = Required(str, default='0')
        diastolicBP = Required(str, default='0')
        normal = Required(str, default='Y')
        HTN = Required(str, default='N')
        preHTN = Required(str, default='N')
        controlled = Required(str, default='N')
        weight = Required(str, default='0')
        height = Required(str, default='0')
        diabetic = Required(str, default='N')
        smoker = Required(str, default='N')
        alcohol = Required(str, default='N')
        isLifestyleTreatment = Required(str, default='N')
        isMedicineTreatment = Required(str, default='N')     
        medicationDispensed = Required(str, default='0')
        comments = Required(str, default='0')
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
        Treatment(
            patientName=str(row["Name"]),
            systolicBP=str(row['Systolic BP Reading']),
            diastolicBP=str(row['Diastolic BP Reading']),
            normal=str(row['Normal']),
            HTN=str(row['HTN']),
            preHTN=str(row['Pre_HTN']),
            controlled=str(row['Controlled']),
            weight=str(row['Weight']),
            height=str(row['Height']),
            diabetic=str(row['Diabetic']),
            smoker=str(row['Smoker']),
            alcohol=str(row['Alcohol']),
            isLifestyleTreatment=str(row['Treated With Lifestyle']),
            isMedicineTreatment=str(row['Treated With Medicine']),
            medicationDispensed=str(row['HTN Medication Dispensed']),
            comments=str(row['Comments']),


        )

logger.info('Saving to Database...')

save()








    



            
