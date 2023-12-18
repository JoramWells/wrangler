import pandas as pd

df = pd.read_csv('./data/HBPressure/patients.csv')


columnNames = df.columns.tolist()

column_names_df = pd.DataFrame({'ColumnNames': columnNames})
column_names_df.to_csv('column_names.csv', index=False)
