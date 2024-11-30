# import and save NYC Jobs data from Open Data

import numpy as np
import pandas as pd
# from socrata.authorization import Authorization
# from socrata import Socrata 
import requests
from datetime import datetime

odata_url = "https://data.cityofnewyork.us/resource/pda4-rgn4.json"

df = pd.read_json(odata_url+"?$limit=10000")

def dl_data(url="https://data.cityofnewyork.us/resource/pda4-rgn4.json"):
    """
    Downloads data from NYC Open Data Socrata API and returns it as a pandas DataFrame
    
    Parameters:
    url (str): The URL for the Socrata API endpoint
    
    Returns:
    pandas.DataFrame: The downloaded data as a DataFrame
    str: The timestamp when the data was downloaded
    """
    try:



        # Download data
        df = pd.read_json(url+"?$limit=10000")
        
        # Save DataFrame to CSV
        csv_filename = f'nyc_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(f'raw/'+csv_filename, index=False)
        
        print(f"CSV saved to {csv_filename}")
        print(f"Number of records downloaded: {len(df)}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return None, None
    
df = dl_data()