#File to run exploratory data analysis on vaccination and disease data
from datetime import datetime
from datetime import timezone as timz
import pandas as pd
import boto3
import io
import os
import json

# S3 config
BUCKET = "vacc-disease-mlops-pipeline-argh"
s3 = boto3.client("s3")
# Getting timestamp date for files.
tmstamp = datetime.now(tz=timz.utc).strftime("%Y%m%d")

#Function to load vaccination and disease data
def load_vacc_disease_data():
    print ("Loading vaccination and disease data...")
    vacc_df = pd.DataFrame()
    disease_df = pd.DataFrame()
    
    try:
        vacc_key = f"processed/vaccination/processed_vaccination_{tmstamp}.csv"
        disease_key = f"processed/disease/processed_disease_{tmstamp}.csv"
        vacc_obj = s3.get_object(Bucket=BUCKET, Key=vacc_key)
        disease_obj = s3.get_object(Bucket=BUCKET, Key=disease_key)
        vacc_df = pd.read_csv(io.BytesIO(vacc_obj["Body"].read()), low_memory=False)
        print("-> Total vaccination records:", len(vacc_df))
        disease_df = pd.read_csv(io.BytesIO(disease_obj["Body"].read()), low_memory=False)
        print("-> Total disease records:", len(disease_df))
        print("✅ Vaccination and disease data loaded successfully.")
    except Exception as e:
        print(f"❌ Error loading vaccination data: {e}")
        return vacc_df, disease_df
    return vacc_df, disease_df

#Funtion to assign country names and continents.
def get_country_name(df):
    print("Adding country names to the data...")
    # Load country codes mapping
    if df.empty:
        print("❌ DataFrame is empty, cannot add country names.")
        return df
    
    ctry_key = "country_codes/country_codes.csv"
    #Loading countries from S3
    count_obj = s3.get_object(Bucket=BUCKET, Key=ctry_key)
    country_codes = pd.read_csv(io.BytesIO(count_obj["Body"].read()), low_memory=False, sep=",", on_bad_lines='skip')    
    df = df.merge(country_codes, left_on="country", right_on="code_3", how="left", suffixes=("", "_x"))
    df = df.drop(columns=["code_3"], errors="ignore")
    df.rename(columns={"country_x": "country_name"}, inplace=True)

    print("✅ Country names added.")
    return df

# Function to execute EDA on vaccination and disease data
def execute_data_improvement(vacc_df, disease_df):
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

    # Changing column types
    combined_df["value"] = pd.to_numeric(combined_df["value"], errors="coerce").astype("float")
    combined_df["year"] = pd.to_numeric(combined_df["year"], errors="coerce").astype("Int64")
    combined_df[["indicator", "country", "region", "disease_name","country_name","type", "continent"]] = combined_df[[
        "indicator", "country", "region", "disease_name","country_name","type", "continent"
        ]].astype("string")

    print("Combined Data types:")
    print(combined_df.dtypes)
    #Adding data consistency.

    for col in combined_df:
        if (col not in(["indicator", "value", "year"])):            
            combined_df[col] = combined_df[col].str.strip().str.title()

    #Data cleaned and well structured.
    print(combined_df.head(20))
    return combined_df

#Function to detect and clean outliers in data.
def det_clean_outliers(df_dvc):
    #declaring json to track changes
    log = {}
 
    # Detect and remove outliers per disease_code
    cleaned_df = pd.DataFrame()
    
    for indcr in df_dvc["indicator"].unique():
        sub_df = df_dvc[df_dvc["indicator"] == indcr].copy()
        #Getting outliers
        q1 = sub_df["value"].quantile(0.25)
        q3 = sub_df["value"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        before = len(sub_df)
        print("Total records before trimming for " + indcr + ": " + str(before))
        sub_df = sub_df[(sub_df["value"] >= lower_bound) & (sub_df["value"] <= upper_bound)]
        after = len(sub_df)
        print("Total records after trimming for " + indcr + ": " + str(after))

        log[indcr] = {
            "initial_records": before,
            "cleaned_records": after,
            "removed_outliers": before - after,
            "lower_bound": round(lower_bound, 2),
            "upper_bound": round(upper_bound, 2)
        }
        cleaned_df = pd.concat([cleaned_df, sub_df], ignore_index=True)

    print(f"✅ Outlier cleaning complete.")
    return (cleaned_df, log)

#Funtion to uploaded cleaned data back to S3
def s3_store_cleaned_data(cleaned_df, jsonlog):
    KEY_CSV = f"processed/forecasting/cleaned_for_forecast_{tmstamp}.csv"
    KEY_LOG = f"logs/eda/outlier_summary_{tmstamp}.json"

    # --- Upload CSV ---
    csv_buffer = io.StringIO()
    cleaned_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=KEY_CSV, Body=csv_buffer.getvalue())
    print(f"✅ Cleaned data uploaded to S3 → s3://{BUCKET}/{KEY_CSV}")

    # --- Upload Log ---
    log_str = json.dumps(jsonlog, indent=2)
    s3.put_object(Bucket=BUCKET, Key=KEY_LOG, Body=log_str)
    print(f"📘 Log uploaded to S3 → s3://{BUCKET}/{KEY_LOG}")

    print("Data cleaned stored successfully")

# Function to perform EDA on vaccination and disease data
def eda_analysis_data():
    vacc_df, disease_df = load_vacc_disease_data()
    if vacc_df.empty or disease_df.empty:
        print("❌ No data available for EDA.")
        return
    else:
        df_all = execute_data_improvement(vacc_df, disease_df)
        if (df_all.notnull):
            out_cleaned, jsonlog = det_clean_outliers(df_all)
            if (out_cleaned.empty):
              print("❌ Data cleaned emtpy. Process finished with errors")
              return                
            else:
                s3_store_cleaned_data(out_cleaned, jsonlog)
        else: 
            print("❌ Data combined emtpy. Process finished with errors")

#Lambda handler
def lambda_handler():
    print("Starting EDA on vaccination and disease data...")
    eda_analysis_data()
    print("EDA on vaccination and disease finished")
    return {
        "statusCode": 200,
        "body": json.dumps("✅ Cleaning complete.")
    }