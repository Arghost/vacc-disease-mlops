import pandas as pd
import boto3
from datetime import datetime
import re
from io import StringIO

# AWS config
s3 = boto3.client("s3")
S3_BUCKET = "vacc-disease-mlops-pipeline-argh"
INPUT_PREFIX = "processed/forecasting/"
OUTPUT_PREFIX = "aggregated/forecasting/"

def get_latest_file():
    """Fetch the most recent file from the forecasting input folder."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=INPUT_PREFIX)
    files = [obj['Key'] for obj in response.get('Contents', []) if re.search(r"cleaned_for_forecast_(\d{8})\.csv$", obj['Key'])]
    latest = max(files, key=lambda x: re.search(r"(\d{8})", x).group(1))
    return latest

def load_from_s3(key):
    """Download a CSV file from S3 into a DataFrame."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_csv(response['Body'])
    return df

def detect_anomalies(df):
    """Detect year-over-year changes indicating potential anomalies."""
    df_sorted = df.sort_values(by=["country_name", "disease_name", "type", "year"])
    df_sorted["value_prev"] = df_sorted.groupby(["country_name", "disease_name", "type"])["value"].shift(1)
    df_sorted["change_pct"] = (df_sorted["value"] - df_sorted["value_prev"]) / df_sorted["value_prev"]
    
    # Define thresholds (can be tuned)
    df_sorted["anomaly"] = df_sorted["change_pct"].apply(
        lambda x: "sudden_spike" if x > 0.80 else "sudden_drop" if x < -0.80 else None
    )
    return df_sorted.drop(columns=["value_prev"])

def save_to_s3(df):
    """Save the DataFrame to a timestamped CSV in the output folder on S3."""
    timestamp = datetime.now().strftime("%Y%m%d")
    key = f"{OUTPUT_PREFIX}grouped_combined_data_{timestamp}.csv"
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=S3_BUCKET, Key=key)
    print(f"✅ Uploaded to → s3://{S3_BUCKET}/{key}")

def lambda_handler(event=None, context=None):
    print("🚀 Starting aggregation and anomaly detection...")

    latest_key = get_latest_file()
    print(f"📥 Latest input: {latest_key}")
    
    df = load_from_s3(latest_key)
    print(f"✅ Data loaded from S3")

    # Group and aggregate
    grouped = df.groupby(["country_name", "year", "type", "disease_name"], as_index=False).agg({
        "value": "mean"
    })

    # Merge region and continent back in for context
    region_info = df[["country_name", "region", "continent"]].drop_duplicates()
    grouped = grouped.merge(region_info, on="country_name", how="left")

    # Anomaly detection
    flagged = detect_anomalies(grouped)
    print(f"✅ Data grouped and anomalies have been reviewed")

    # Output
    save_to_s3(flagged)

    return {
        "statusCode": 200,
        "body": "✅ Aggregation + anomaly tagging complete."
    }