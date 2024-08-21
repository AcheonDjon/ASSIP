import pandas as pd
import os as os
source_folder = '/Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data'
df2 = pd.read_csv('bigquery/Phase 2_Research projects.xlsx - Data.csv')
df3 = df2.iloc[144:164,3]
for d in os.listdir(source_folder):
    df1 = pd.read_csv(f'/Users/zebra/Documents/projects/ASSIP_Data/Even_Newer_Data/{d}')
    dfinal = pd.concat([df1, df3], axis=1)
    dfinal.to_csv(f'/Users/zebra/Documents/projects/ASSIP_Data/With_Company_Metrics/{d}_updated', index=False)
# for f in os.listdir(source_folder): 
#     df = pd.read_csv(f)

