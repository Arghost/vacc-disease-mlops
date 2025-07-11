import json
import os
import requests
import boto3
import pandas as pd

# Vaccination indicators (WHO API codes)
VACCINE_INDICATORS = {
    "DTP3": "WHS4_100",
    "MMR": "WHS8_110",
    "Pol3": "WHS4_544",
    "HepB3": "WHS4_117"
}

# Disease indicators (reported cases or incidence)
DISEASE_INDICATORS = {
    "measles": "WHS3_62",
    "diphtheria": "WHS3_41",
    "polio": "WHS3_49",
    "hepatitis_b": "HEPATITIS_HBV_INFECTIONS_NEW_NUM"
}

# WHO API base
BASE_URL = "https://ghoapi.azureedge.net/api/"

# Your S3 bucket
S3_BUCKET = "vacc-disease-mlops-pipeline-argh"
s3 = boto3.client("s3")

def download_and_upload(category, name, code):
    url = f"{BASE_URL}{code}?$format=json"
    print(f"🔄 Fetching {name} data from {url}")
    try:
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json().get("value", [])
            df = pd.DataFrame(data)
            key = f"raw/{category}/{name}.csv"
            csv_buffer = df.to_csv(index=False)
            s3.put_object(Body=csv_buffer, Bucket=S3_BUCKET, Key=key)
            print(f"✅ Uploaded to S3 → {key}")
        else:
            print(f"❌ HTTP {r.status_code} error for {name}")
    except Exception as e:
        print(f"❌ Error downloading {name}: {e}")

def lambda_handler(event=None, context=None):
    print("🚀 Starting ingestion process...")

    for name, code in VACCINE_INDICATORS.items():
        download_and_upload("vaccination", name, code)

    for name, code in DISEASE_INDICATORS.items():
        download_and_upload("disease", name, code)

    print("✅ All data ingested and uploaded to S3.")
    return {
        "statusCode": 200,
        "body": json.dumps("✅ Data ingestion complete.")
    }