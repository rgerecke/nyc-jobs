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
        .sample(n=6000,seed=47)
)

df_mut.get_column('comms').value_counts()

# %%

# takes about 
docs = list(nlp.pipe(df_mut.get_column('job_description').to_list()))

for token in doc:
    print(token.text, token.dep)

for ent in doc.ents:
    print(ent.text, ent.label_)

# %%