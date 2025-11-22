import pandas as pd
import os

# Path to CSV file we want to read from
file_path = "orders.csv"

# Path to output directory to store split data
output_dir = "data/raw"

def main():
    # Load the dataset
    df = pd.read_csv(file_path)

    # Loop through 0 to 6 to store orders corresponding to days 0 to 6
    for dow in range(7):
        
        # Build the path to the folder that stores the CSV file of dow e.g. "data/raw/{dow}"
        folder = os.path.join(output_dir, str(dow))
        
        # Create the folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)
        
        # Filter the dataset by "order_dow"
        df_dow = df[df["order_dow"] == dow]
        
        # Build the path to the CSV file of dow
        dow_file = os.path.join(folder, f"orders_{dow}.csv")
        
        # Store the filtered data in the corresponding CSV file
        df_dow.to_csv(dow_file, index=False)
        
        # print success message
        print(f"Successfully created ${dow_file} file with {len(df_dow)} rows!")
        
if __name__ == '__main__':
    main()