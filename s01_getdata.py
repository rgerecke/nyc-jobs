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

    date_string = datetime.now().strftime("%Y-%m-%d")

    od_df = socrata.socrata_api_query(
        dataset_id='kpav-sd4t', 
        timeout=300,
        limit=10000
        )
    
    od_df = od_df.with_columns(pl.lit(date_string).alias("updated"))

    print(f'{len(od_df)} records downloaded on {date_string}')

    return od_df

# %%

def update_database(
    new_df,
    old_db
):
    """
    Add new data to existing database of NYC Jobs.
    Matched using the `job_id` and `posting_type` fields;
    duplicates are replaced with more recent information.
    """

    removed_df = (
        old_db
            .join(new_df, on = ['job_id', 'posting_type'], how = 'anti')
            .select(new_df.columns)
    )

    new_db = pl.concat([new_df, removed_df], how = "vertical")

    print(f"{len(removed_df)} retained records.")
    print(f"Current updated distribution: {new_db.get_column('updated').value_counts(sort = True)}")

    return new_db

# %%

if __name__ == '__main__':
    old_db = pl.read_csv("/mnt/data/database/database.csv", infer_schema=False)

    new_df = get_nyc_jobs()

    new_db = update_database(new_df, old_db)

    new_db.write_csv("database.csv")


