# Dedupe data
# import numpy as np
# import pandas as pd

import polars as pl
import os

filenames = ['raw/' + x for x in os.listdir('raw')]

df_old = pl.read_csv(filenames[1])
df_new = pl.read_csv(filenames[0])


# job records are unique to the job_id and posting_type combination
# the same job can be posted both internally and externally
# when joining, want to retain the latest version of each job posting

# first, find rows that are no longer in the jobs dataset
df_removed = df_old.join(df_new, on = ['job_id', 'posting_type'], how = 'anti').select(df_new.columns)

df_combined = pl.concat([df_new, df_removed], how = 'vertical')

