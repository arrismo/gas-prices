import pandas as pd
import glob
import os

# Method 1: Join all CSVs in a directory
def join_csv_files():
    # Use glob to get all CSV files in the directory
    all_files = glob.glob('data/*.csv')
   
    # Read and concatenate all files
    df_list = [pd.read_csv(file) for file in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
   
    return combined_df

def save_combined_csv(combined_df, output_filename):
    combined_df.to_csv(output_filename, index=False)

# Example usage
if __name__ == "__main__":
    # Option 1: Join all CSVs in a directory
    combined_df = join_csv_files()
   
    # Save the combined file
    save_combined_csv(combined_df, 'combined_output.csv')