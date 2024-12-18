# %%
import polars as pl
import spacy

df = pl.read_csv("database.csv")
# %%
(
    df.get_column("job_category")
        .value_counts(sort = True)
        .write_csv("job_category.csv")
)
# %%

# yo these guys are fucking dumb

df_mut = df.select(["job_id", "posting_type", "job_category"]).to_dummies(columns="job_category", separator=",")

# %%