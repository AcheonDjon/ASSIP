import os
import pandas as pd
bitcoin = ['0xcregis', 'dethertech', 'KMPARDS', 'fr8network', 'Beaboutyourlife', 'impactMarket', 'linear-protocol','MindCoinTeam','niftyx','PanIndustrial-Org','PrimLabs', 'RubiconDeFi', 'Solanascan','ImpeccableHQ','TrSoft-Inc', 'wannaswap']
for b in bitcoin:
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

    # Define the input folder and output file
    input_folder = f"/Users/zebra/Documents/projects/ASSIP_Data/All_data/{b}"
    output_file = f"/Users/zebra/Documents/projects/ASSIP_Data/All_data/{b}_project_.csv"

    # Combine the CSV files
    combine_csv_files(input_folder, output_file)