import pandas as pd

df = pd.read_csv('./data/HURUMA HOSPITAL FEBRUARY 2023 B.P SCREENING DATA.csv')

columns = ['Name of Client','Systolic BP Reading', 'New',
           'Known HTN','Taking meds','B.P','Normal','High B.P','Referred',
           'Referral No.','Facility referred','Date']

columnList =[x for x in df.columns if x in columns]

new_df = df[columnList]

print(new_df)

new_df.to_csv('./patientScreeningFeb.csv')


# df.drop(columns='ID', inplace=True)