import pandas as pd

# Load the two CSV files into Pandas DataFrames
df1 = pd.read_csv('./data/HBPressure/patientTreatmentDetail.csv')
df2 = pd.read_csv('./data/HBPressure/patientTreatmentFeb.csv')
# df1['Name of Client'] = df1['Name of Client'].astype(str)
# df2['Name of Client'] = df2['Name of Client'].astype(str)

df2.drop(columns='ID', inplace=True)


# Merge the DataFrames based on the 'ID' column
merged_df = pd.concat([df1, df2], ignore_index=True)

# Display the merged DataFrame
print("Merged DataFrame:")
print(merged_df)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('./JanFebTreatment.csv', index=False)