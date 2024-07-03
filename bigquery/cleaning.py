import pandas as pd
import os
folderpath = '/Users/zebra/Documents/projects/ASSIP_Data/New_Data/combined'
for filename in os.listdir(folderpath):
    filepath = os.path.join(folderpath, filename)
    df = pd.read_csv(filepath)
    if df.duplicated().any():
         print("Duplicates exist. Removing duplicates.")
         df_cleaned = df.drop_duplicates()
         iloc = df_cleaned.iloc[:, 4:]   
         column_means = iloc.mean()
         if (column_means < 0).all():
             print(f"Warning: Negative values found in {filename}")
    
# If you want to remove duplicates and keep the last occurrence instead of the first
# df_cleaned = df.drop_duplicates(keep='last')

# To remove duplicates based on specific columns only, specify the subset parameter
# df_cleaned = df.drop_duplicates(subset=['column_name1', 'column_name2'], keep='first')

# Save the cleaned DataFrame back to a CSV file, if needed
# df_cleaned.to_csv('your_cleaned_dataset.csv', index=False) 