# import and save NYC Jobs data from Open Data

from socrata import socrata_api_query
from datetime import datetime

if __name__ == '__main__':
    print('NYC job postings active this week')
    start_time = time.time()
    od_df = socrata_api_query(
        dataset_id='pda4-rgn4', 
        timeout=300,
        limit=10000
        )
    end_time = time.time()
    len_sec = end_time - start_time
    min_time = math.floor(len_sec / 60)
    print(
        f'Duration: {min_time} min'
        f' {len_sec - min_time * 60:.4} sec'
    )
    
    csv_filename = f'nyc_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    od_df.to_csv(f'raw/{csv_filename}', index=False)
        
    print(f"CSV saved to {csv_filename}")
    print(f"Number of records downloaded: {len(od_df)}")