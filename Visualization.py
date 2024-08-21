import pandas as pd 
import matplotlib.pyplot as plt
path = '/Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data/aeonix_project_54.csv'
name = "Aeonix: Number of Activities vs Date"
aeonix_df = pd.read_csv(path)
plt.figure(figsize=(10, 6))
aeonix_df['Date'] = pd.to_datetime(aeonix_df['date1'])
aeonix_df.set_index('Date', inplace=True)
monthly_activities = aeonix_df.resample('M', on='Date').mean()
plt.scatter (aeonix_df['Date'], aeonix_df['num_activities'])
plt.title('Line Plot')
plt.xlabel('Date')
plt.ylabel('Number of Activities')
plt.grid(True)
plt.savefig(f'/Users/zebra/Documents/projects/ASSIP_Data/bigquery/Graphs/{name}')