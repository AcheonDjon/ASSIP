import pandas as pd
import os
import re
             # Define the input folder and output file

def combine_csv_files(input_folder, output_file):
                # List to hold DataFrames
                data_frames = []

                # Iterate over all files in the input folder
                for filename in os.listdir(input_folder):
                    if filename.endswith(".csv"):
                        file_path = os.path.join(input_folder, filename)
                        # Read the CSV file into a DataFrame
                        df = pd.read_csv(file_path)
                        # Append the DataFrame to the list
                        data_frames.append(df)

                # Concatenate all DataFrames
                combined_df = pd.concat(data_frames, ignore_index=True)

                # Write the combined DataFrame to a new CSV file
                combined_df.to_csv(output_file, index=False)
                print(f"Combined CSV saved to {output_file}")

   
    
    # Combine the CSV files
bitcoin = ['casinolandnetwork', 'staratlasmeta', 'quark-project']


for b in bitcoin:
    input_folder = f"/Users/zebra/Documents/projects/ASSIP_Data/New_Data/{b}"
    output_file = f"/Users/zebra/Documents/projects/ASSIP_Data/New_Data/{b}_project_.csv" 
    combine_csv_files(input_folder, output_file)
                # # Export results to CSV
            # with open(csv_file_path, "w") as csv_file:
            # # Write headers
            #     csv_file.write(",".join([field.name for field in results.schema]) + "\n")
        
            # # Write data rows
            # for row in results:
            #     csv_file.write(",".join([str(value) for value in row]) + "\n")


