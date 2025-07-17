#File to run exploratory data analysis on vaccination and disease data
from datetime import datetime
from datetime import timezone as timz
import pandas as pd
import boto3
import io

# S3 config
bucket = "vacc-disease-mlops-pipeline-argh"
s3 = boto3.client("s3")

#Function to load vaccination and disease data
def load_vacc_disease_data():
    print ("Loading vaccination and disease data...")
    vacc_df = pd.DataFrame()
    disease_df = pd.DataFrame()
    # Create timestamp
    tmstamp = datetime.now(tz=timz.utc).strftime("%Y%m")
    try:
        vacc_key = f"processed/vaccination/processed_vaccination_{tmstamp}.csv"
        disease_key = f"processed/disease/processed_disease_{tmstamp}.csv"

        vacc_obj = s3.get_object(Bucket=bucket, Key=vacc_key)
        disease_obj = s3.get_object(Bucket=bucket, Key=disease_key)
        vacc_df = pd.read_csv(io.BytesIO(vacc_obj["Body"].read()), low_memory=False)
        print("-> Total vaccination records:", len(vacc_df))
        disease_df = pd.read_csv(io.BytesIO(disease_obj["Body"].read()), low_memory=False)
        print("-> Total disease records:", len(disease_df))
        print("✅ Vaccination and disease data loaded successfully.")
    except Exception as e:
        print(f"❌ Error loading vaccination data: {e}")
        return vacc_df, disease_df
    return vacc_df, disease_df

# Function to execute EDA on vaccination and disease data
def execute_eda(vacc_df, disease_df):
    print("✅ To be continued....")

# Function to perform EDA on vaccination and disease data
def eda_analysis_data():
    vacc_df, disease_df = load_vacc_disease_data()
    if vacc_df.empty or disease_df.empty:
        print("❌ No data available for EDA.")
        return
    else:
        execute_eda(vacc_df, disease_df)

if __name__ == "__main__":
    print("Starting EDA on vaccination and disease data...")
    eda_analysis_data()
    print("EDA on vaccination and disease finished")