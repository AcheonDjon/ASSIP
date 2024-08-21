import pandas as pd
import os

folder_path = '/Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data'  # Update this to your actual folder path
combined_csv_path = '//Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data/combined_file.csv'  # Update this to your desired output file path

# List all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Initialize an empty list to store DataFrames
dfs = []

for file in csv_files:
    file_path = os.path.join(folder_path, file)
    # print(file)
    #get just the name of the part of the file before the first underscore
    name = file.split('_')[0]
    # print(name)

    #Read the CSV file, add filename as the first column
    df = pd.read_csv(file_path, header=0)
    df.insert(0, 'Filename', name.lower())

    #exclude the first row
    df = df.iloc[1:]
    dfs.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a new CSV file
combined_df.to_csv(combined_csv_path, index=False)