# import and save NYC Jobs data from Open Data
# %%

import socrata
from datetime import datetime
import polars as pl

# %%
def get_nyc_jobs():
    """
    Pull latest data from NYC jobs using the Socrata API and
    add 'updated' column with date of download as YYYYMMDD.
    """

    date_string = datetime.now().strftime("%Y%m%d")

    od_df = socrata.socrata_api_query(
        dataset_id='pda4-rgn4', 
        timeout=300,
        limit=10000
        )
    
    od_df = od_df.with_columns(pl.lit(date_string).alias("updated"))

    return od_df

# %%

if __name__ == '__main__':
    old_db = pl.read_csv("database.csv")

    od_df = socrata.socrata_api_query(
        dataset_id='pda4-rgn4', 
        timeout=300,
        limit=10000
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