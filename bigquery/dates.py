import pandas as pd
import os

folderpath = "/Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data"

# Check if the folder path exists

for filename in os.listdir(folderpath):
    filepath = os.path.join(folderpath, filename)
    # Ensure the 'date' column is parsed as datetime
    df = pd.read_csv(filepath, parse_dates=['date1'])
    
    # Find the min and max dates
    min_date = df['date1'].min()
    max_date = df['date1'].max()
    
    # Print the min and max dates
    print(f"File: {filename} - Min Date: {min_date}, Max Date: {max_date}")