# Job Categorizer

## Overview
The Job Categorizer project aims to classify job descriptions into specific categories based on the provided business titles and civil service titles. This project utilizes spaCy, a powerful NLP library, to preprocess the data, train a model, and make predictions.

## Project Structure
```
job-categorizer
├── src
│   ├── preprocess.py        # Data cleaning and preprocessing functions
│   ├── dataset.py           # Class for loading and preparing job data
│   ├── train.py             # Training logic for the spaCy model
│   └── predict.py           # Functions for loading the model and making predictions
├── config
│   └── config.cfg           # Configuration file for spaCy model parameters
├── data
│   ├── raw
│   │   └── jobs.csv         # Raw job data with descriptions and titles
│   └── processed
│       └── train.spacy      # Processed training data in spaCy format
├── notebooks
│   └── exploration.ipynb     # Jupyter notebook for exploratory data analysis
├── scripts
│   └── convert_to_spacy.py  # Script to convert CSV data to spaCy format
├── requirements.txt          # Python dependencies for the project
├── .gitignore                # Files and directories to ignore by Git
└── README.md                 # Documentation for the project
```

## Setup Instructions
1. Clone the repository:
   ```
   git clone <repository-url>
   cd job-categorizer
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Prepare the data:
   - Place your raw job data in the `data/raw` directory as `jobs.csv`.
   - Run the conversion script to prepare the data for training:
     ```
     python scripts/convert_to_spacy.py
     ```

4. Train the model:
   ```
   python src/train.py
   ```

5. Make predictions:
   ```
   python src/predict.py
   ```

## Usage
- Use the `exploration.ipynb` notebook for data analysis and visualization.
- Modify the configuration in `config/config.cfg` to adjust model parameters and training settings.

## Goals
- To create a robust model that accurately categorizes job descriptions.
- To facilitate easy updates and modifications to the model and data processing pipeline.