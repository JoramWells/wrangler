import pandas as pd

df = pd.read_csv('./data/HBPressure/JanFeb.csv')
# df2['Unique Client ID'] = df1['Unique Client ID'].astype(str)


print(df['Unique Client ID'].duplicated().sum())

df.drop_duplicates(subset='Unique Client ID', keep='first', inplace=True)

df.to_csv('patientsJanFebNoDuplicates.csv', index=False)

# columnNames = df.columns.tolist()

# column_names_df = pd.DataFrame({'ColumnNames': columnNames})
# column_names_df.to_csv('column_names.csv', index=False)

# print(df[df['Name of Client'].duplicated() == True])