import boto3
import pandas as pd
import io
import os
from datetime import datetime
from datetime import timezone as timz

# S3 config
bucket = "vacc-disease-mlops-pipeline-argh"
s3 = boto3.client("s3")

def list_s3_files(prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def download_csv(key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def process_category(category):
    prefix = f"raw/{category}/"
    files = list_s3_files(prefix)
    dfs = []

    for key in files:
        df = download_csv(key)
        df["indicator_code"] = os.path.basename(key).split("_")[0]
        dfs.append(df)

    if not dfs:
        print(f"❌ No files found for {category}")
        return

    df_all = pd.concat(dfs, ignore_index=True)
    #save local file for testing
    df_cleaned = df_all.rename(columns={
        "IndicatorCode": "indicator",
        "SpatialDim": "country",
        "ParentLocation": "region",   # region can be null — that’s okay
        "TimeDim": "year",
        "Value": "value",
        "indicator_code": "disease_code"
    })
    df_cleaned = df_cleaned[["indicator", "country", "region", "year", "value", "disease_code"]]
    #Remove rows with null values
    df_cleaned = df_cleaned.dropna(subset=["indicator", "year", "country", "value", "disease_code"])

    # Create timestamp
    timestamp = tmstamp = datetime.now(tz=timz.utc).strftime("%Y%m%d")

    # 1️⃣ Upload versioned cleaned file
    clean_key = f"processed/{category}/processed_{category}_{timestamp}.csv"
    buffer = io.StringIO()
    df_cleaned.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=clean_key, Body=buffer.getvalue())
    print(f"✅ Uploaded cleaned file → {clean_key}")

    # 2️⃣ Append to or create master dataset
    agg_key = f"aggregated/{category}/master_{category}.csv"
    try:
        existing_df = download_csv(agg_key)
        df_combined = pd.concat([existing_df, df_cleaned]).drop_duplicates()
    except Exception:
        print(f"ℹ️ No existing {category} master file found — creating new.")
        df_combined = df_cleaned

    agg_buffer = io.StringIO()
    df_combined.to_csv(agg_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=agg_key, Body=agg_buffer.getvalue())
    print(f"✅ Master dataset updated → {agg_key}")

if __name__ == "__main__":
    process_category("vaccination")
    process_category("disease")
    print("✅ Data processing complete.")