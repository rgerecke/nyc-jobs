import pandas as pd
import spacy
from spacy.tokens import DocBin, Doc

def convert_to_spacy(input_csv, output_spacy):
    # Load the raw job data
    df = pd.read_csv(input_csv)

    # Initialize a blank spaCy model
    nlp = spacy.blank("en")
    doc_bin = DocBin()

    # Register custom attributes if not already registered
    Doc.set_extension('business_title', default=None)
    Doc.set_extension('civil_service_title', default=None)

    for _, row in df.iterrows():
        # Create a spaCy Doc object
        doc = nlp(row['job_description'])
        
        # Add custom attributes for business title and civil service title
        doc._.business_title = row['business_title']
        doc._.civil_service_title = row['civil_service_title']
        
        # Add the Doc to the DocBin
        doc_bin.add(doc)

    # Save the DocBin to the specified output file
    doc_bin.to_disk(output_spacy)

if __name__ == "__main__":
    input_csv = 'data/raw/jobs.csv'
    output_spacy = 'data/processed/train.spacy'
    convert_to_spacy(input_csv, output_spacy)