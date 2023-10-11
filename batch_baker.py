import os
import pandas as pd
import requests
from io import StringIO
import re

def extract_all_numbers(s):
    return ''.join(re.findall(r'\d', s))

def batch_please(df, name, batch_size=9999):
    name = extract_all_numbers(name)
    num_batches = len(df) // batch_size + 1
    if not os.path.exists("batches"):
        os.mkdir("batches")
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        df_batch = df.iloc[start_idx:end_idx]
        df_batch.to_csv(f"batches/batch_{name}_{i}.csv", index=False, quoting=2)



###

url = "data/swvf_s4.csv"

df = pd.read_csv(url, dtype=str)\
    .filter(items=["SOS_VOTERID","RESIDENTIAL_ADDRESS1","RESIDENTIAL_CITY","RESIDENTIAL_STATE","RESIDENTIAL_ZIP"])\
    .assign(RESIDENTIAL_ZIP=lambda df: df["RESIDENTIAL_ZIP"].astype("str"))

df.columns = ["SOS_VOTERID","Street address","City","State","Zip"]

batch_please(df, name=url)

