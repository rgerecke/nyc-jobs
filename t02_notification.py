# %%
import socrata
from datetime import datetime
import polars as pl
import os

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

    od_df = od_df.with_columns([
            pl.col("salary_range_from")
              .cast(pl.Float64)
              .alias("salary_range_from"),
            pl.col("salary_range_to")
              .cast(pl.Float64)
              .alias("salary_range_to"),
            pl.lit(date_string).alias("updated")
        ])

    print(f'{len(od_df)} records downloaded on {date_string}')

    return od_df

# %%

df = get_nyc_jobs()

