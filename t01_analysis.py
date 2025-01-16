# %%
import polars as pl
import spacy
from spacy.tokens import DocBin

df = pl.read_csv("database.csv")
nlp = spacy.load("en_core_web_sm")

# %%
(
    df.get_column("job_category")
        .value_counts(sort = True)
        .write_csv("job_category.csv")
)
# %%

df_mut = (
    df.with_columns(
        pl.col('job_category')
            .str.contains("Communications")
            .alias('comms')
    )
        .sample(n=500,seed=47)
)

df_mut.get_column('comms').value_counts()

# %%

data_tuples = df_mut.select(['job_description', 'comms']).rows()
doc_bin = DocBin()



# yo these guys are fucking dumb

# df_mut = df.select(["job_id", "posting_type", "job_category"]).to_dummies(columns="job_category", separator=",")

# %%