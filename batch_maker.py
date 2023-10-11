import requests
import os
import pandas as pd

os.chdir("c:\\Users\\Aj\\Desktop\\School\\DS499\\app")

def process_batch(batch_path):
    url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
    with open(batch_path, 'rb') as f:
        response = requests.post(url, files={'addressFile': f}, data={
            'returntype': 'locations',
            'benchmark': 'Public_AR_Current' 
        })
    response_data = response.text
    lines = response_data.strip().split("\n")
    data = [line.strip('"').split('","') for line in lines]
    finished_df = pd.DataFrame(data, columns=["SOS_VOTERID", "Street_address","Match", "Match_type", "Matched_address", "Latitude_Longitude", "Tiger_line_id", "Tiger_line_side"])
    return finished_df
 
 
 # Create finished_batches directory if it doesn't exist
if not os.path.exists("finished_batches"):
    os.makedirs("finished_batches")

# Loop through items in the batches directory
for batch_file in os.listdir("batches"):
    batch_path = os.path.join("batches", batch_file)

    # Check if the item is a directory
    if os.path.isdir(batch_path):
        # Extract the batch identifier (like "a" from "batch_1_50a")
        batch_identifier = batch_file.split("_")[-1]

        # If it's a directory, then loop through its contents to find the CSVs
        for csv_file in os.listdir(batch_path):
            if csv_file.endswith(".csv"):
                csv_path = os.path.join(batch_path, csv_file)
                print(f"Processing {csv_path}")
                
                finished_df = process_batch(csv_path)
                # Including the batch identifier in the finished file name
                finished_file_name = f"{csv_file.split('.csv')[0]}_{batch_identifier}.csv"
                finished_file_path = os.path.join("finished_batches", finished_file_name)
                finished_df.to_csv(finished_file_path, index=False, quoting=2)

