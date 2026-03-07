def clean_text(text):
    # Function to clean and normalize text
    text = text.strip().lower()  # Convert to lowercase and strip whitespace
    # Additional cleaning steps can be added here
    return text

def handle_missing_values(df):
    # Function to handle missing values in the DataFrame
    df.fillna('', inplace=True)  # Replace NaN with empty strings
    return df

def preprocess_data(df):
    # Function to preprocess the job data
    df = handle_missing_values(df)
    df['job_description'] = df['job_description'].apply(clean_text)
    df['business_title'] = df['business_title'].apply(clean_text)
    df['civil_service_title'] = df['civil_service_title'].apply(clean_text)
    return df