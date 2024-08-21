import pandas as pd

# Load the CSV file
df = pd.read_csv('/Users/zebra/Documents/projects/ASSIP_Data/bigquery/data_summary.csv')

# Assuming the first column is named after the first entry 'Aeonix' (adjust the column name as necessary)
# Convert the first column to lowercase
df.iloc[:, 0] = df.iloc[:, 0].str.lower()

# Dump the modified DataFrame to a new CSV file
df.to_csv('/Users/zebra/Documents/projects/ASSIP_Data/bigquery/data_summary_2.csv', index=False)