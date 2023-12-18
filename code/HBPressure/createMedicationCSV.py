import pandas as pd

df = pd.read_csv('./data/patientTreatmentDetail.csv')

columns = ['ID','Systolic BP Reading', 'New',
           'Known HTN','Taking meds','B.P','Normal','High B.P','Referred',
           'Referral No.','Facility referred']

columnList =[x for x in df.columns if x not in  columns]

# new_df = df[columnList]


# print(df['HTN Medication Dispensed'].unique().count())

new_df = pd.DataFrame(df['HTN Medication Dispensed'].value_counts()).reset_index()

new_df.columns = ['HTN Medication Dispensed', 'count']
print(new_df)

new_df.to_csv('./medication.csv')


# df.drop(columns='ID', inplace=True)