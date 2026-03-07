class JobDataset:
    def __init__(self, csv_file):
        import pandas as pd
        self.data = pd.read_csv(csv_file)

    def preprocess(self):
        self.data.dropna(subset=['job_description', 'business_title', 'civil_service_title'], inplace=True)
        self.data['job_description'] = self.data['job_description'].str.lower()
        self.data['business_title'] = self.data['business_title'].str.lower()
        self.data['civil_service_title'] = self.data['civil_service_title'].str.lower()

    def get_data(self):
        return self.data[['job_description', 'business_title', 'civil_service_title']]