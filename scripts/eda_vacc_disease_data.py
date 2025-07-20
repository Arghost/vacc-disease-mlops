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
    tmstamp = datetime.now(tz=timz.utc).strftime("%Y%m%d")
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

def get_country_name(df):
    print("Adding country names to the data...")
    # Load country codes mapping
    if df.empty:
        print("❌ DataFrame is empty, cannot add country names.")
        return df
    
    ctry_key = "country_codes/country_codes.csv"
    #Loading countries from S3
    count_obj = s3.get_object(Bucket=bucket, Key=ctry_key)
    country_codes = pd.read_csv(io.BytesIO(count_obj["Body"].read()), low_memory=False, sep=",", on_bad_lines='skip')    
    df = df.merge(country_codes, left_on="country", right_on="code_3", how="left", suffixes=("", "_x"))
    df = df.drop(columns=["code_3"], errors="ignore")
    df.rename(columns={"country_x": "country_name"}, inplace=True)

    print("✅ Country names added.")
    return df

# Function to execute EDA on vaccination and disease data
def execute_eda(vacc_df, disease_df):
    print("Performing EDA on vaccination and disease data...")
    # Example EDA operations
    print("Vaccination Data Overview:")
    print(vacc_df.describe())
    print("\nDisease Data Overview:")
    print(disease_df.describe())
    
    # Check for missing values
    print("\nMissing Values in Vaccination Data:")
    print(vacc_df.isnull().sum())
    print("\nMissing Values in Disease Data:")
    print(disease_df.isnull().sum())
    
    # Check data types
    print("\nData Types in Vaccination Data:")
    print(vacc_df.dtypes)
    print("\nData Types in Disease Data:")
    print(disease_df.dtypes)
    
    # Additional EDA can be added here
    vacc_df['type'] = 'vaccination'
    disease_df['type'] = 'disease'
    combined_df = pd.concat([vacc_df, disease_df], ignore_index=True)
    print("\nCombined Data Overview:")
    print(combined_df.describe())

    # Changing column types
    combined_df["value"] = pd.to_numeric(combined_df["value"], errors="coerce").astype("float")
    combined_df["year"] = pd.to_numeric(combined_df["year"], errors="coerce").astype("Int64")
    combined_df = combined_df.astype({
        "indicator": "string",
        "country": "string",
        "region": "string",
        "disease_code": "string"
    })
    # Rename column for clarity.
    combined_df = combined_df.rename(columns={"disease_code": "disease_name"})
    combined_df = combined_df.dropna(subset=["value"])
    combined_df = get_country_name(combined_df)
    # Filter rows with specific country codes.
    combined_df = combined_df[~combined_df["country"].isin([
        "AFR",
        "AMR",
        "EMR",
        "EUR",
        "GLOBAL",
        "MDA",
        "SEAR",
        "WB_HI",
        "WB_LI",
        "WB_LMI",
        "WB_UMI",
        "WPR",
        "XKX"
    ])]
    # Assigning region based on country codes
    combined_df.loc[combined_df["country"].isin(["HKG","MAC"]), "region"] = "South-East Asia"
    print("\nTotal NaN values per column in combined_df:")
    print(combined_df.isnull().sum())

    

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