import pandas as pd

df = pd.read_csv('./data/HURUMA HOSPITAL FEBRUARY 2023 B.P SCREENING DATA.csv')

columns = ['ID','Systolic BP Reading', 'New',
           'Known HTN','Taking meds','B.P','Normal','High B.P','Referred',
           'Referral No.','Facility referred']

columnList =[x for x in df.columns if x not in  columns]

new_df = df[columnList]

print(new_df)

new_df.to_csv('./patientsFeb.csv')


# df.drop(columns='ID', inplace=True)