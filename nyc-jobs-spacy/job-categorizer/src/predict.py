from spacy import load
import pandas as pd

def load_model(model_path):
    return load(model_path)

def predict_job_category(model, job_description):
    doc = model(job_description)
    return doc.cats

def main():
    model_path = "path/to/your/model"  # Update with the actual model path
    model = load_model(model_path)
    
    # Example job descriptions for prediction
    job_descriptions = [
        "Software Engineer with experience in Python and machine learning.",
        "Data Analyst skilled in SQL and data visualization.",
    ]
    
    for description in job_descriptions:
        category = predict_job_category(model, description)
        print(f"Job Description: {description}\nPredicted Category: {category}\n")

if __name__ == "__main__":
    main()