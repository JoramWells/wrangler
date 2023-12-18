import pandas as pd

df = pd.read_csv('./data/HURUMA HOSPITAL FEBRUARY 2023 B.P TREATMENT DATA.csv')

columns = ['Name','Systolic BP Reading', 'Diastolic BP Reading',
           'Normal','Pre_HTN','HTN','Controlled','Weight','Height',
           'Diabetic','Smoker','Alcohol','Treated With Lifestyle','Treated With Medicine',
           'HTN Medication Dispensed','Comments'
           ]

columnList =[x for x in df.columns if x in columns]

new_df = df[columnList]

print(new_df)

new_df.to_csv('./patientTreatmentFeb.csv')


# df.drop(columns='ID', inplace=True)