import json
import requests
import boto3
import pandas as pd
from datetime import datetime
from datetime import timezone as timz

# Vaccination indicators (WHO API codes)
VACCINE_INDICATORS = {
    "diphtheria": "WHS4_100",
    "measles": "WHS8_110",
    "polio": "WHS4_544",
    "hepatitis_b": "WHS4_117"
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
            timestamp = datetime.now(tz=timz.utc).strftime("%Y%m%d")
            key = f"raw/{category}/{name}_{timestamp}.csv"
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

if __name__ == "__main__":
    lambda_handler()