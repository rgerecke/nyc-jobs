# Dedupe data
# import numpy as np
# import pandas as pd

import polars as pl
import os
import glob
import re

all_df = pl.DataFrame()
for file_name in sorted(glob.glob('raw/*')):
    print(f'{file_name}')

    date_parse = re.search('\d{8}', file_name).group()
    date_string = f'{date_parse[0:4]}-{date_parse[4:6]}-{date_parse[6:8]}'

    if (len(all_df) == 0):
        all_df = pl.read_csv(file_name).with_columns(pl.lit(date_string).alias("updated"))
    else:
        df_new = pl.read_csv(file_name).with_columns(pl.lit(date_string).alias("updated"))
        df_removed = all_df.join(df_new, on = ['job_id', 'posting_type'], how = 'anti').select(df_new.columns)
        all_df = pl.concat([df_new, df_removed], how = "vertical")

all_df.write_csv("database.csv")

all_df.get_column("updated").value_counts(sort = True)
    # x = pd.read_csv(file_name, low_memory=False)
    # glued_data = pd.concat([glued_data,x],axis=0)


# filenames = ['raw/' + x for x in os.listdir('raw')]

# df_old = pl.read_csv(filenames[1])
# df_new = pl.read_csv(filenames[0])


# # job records are unique to the job_id and posting_type combination
# # the same job can be posted both internally and externally
# # when joining, want to retain the latest version of each job posting

# # first, find rows that are no longer in the jobs dataset
# df_removed = df_old.join(df_new, on = ['job_id', 'posting_type'], how = 'anti').select(df_new.columns)

# df_combined = pl.concat([df_new, df_removed], how = 'vertical')

