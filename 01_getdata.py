# import and save NYC Jobs data from Open Data

import socrata
from datetime import datetime
import polars as pl

if __name__ == '__main__':
    old_db = pl.read_csv("database.csv")

    print('NYC job postings active this week')
    start_time = socrata.time.time()
    od_df = socrata.socrata_api_query(
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
    
    date_string = datetime.now().strftime("%Y%m%d")

    csv_filename = f'nyc_jobs_{date_string}.csv'
    od_df.to_csv(f'raw/{csv_filename}', index=False)
    
    df_new = od_df.with_columns(pl.lit(date_string).alias("updated"))
    df_removed = old_db.join(df_new, on = ['job_id', 'posting_type'], how = 'anti').select(df_new.columns)
    new_db = pl.concat([df_new, df_removed], how = "vertical")
    new_db.write_csv("database.csv")

    print(f"CSV saved to {csv_filename}")
    print(f"Number of records downloaded: {len(od_df)}")
    print(f"Number of new records: {len(new_db) - len(old_db)}")