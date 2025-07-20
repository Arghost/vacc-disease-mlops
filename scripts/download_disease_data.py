import os
import requests
import pandas as pd

# WHO Indicator codes for disease incidence (confirmed working)
DISEASE_INDICATORS = {
    "measles": "WHS3_62",
    "diphtheria": "WHS3_41",
    "polio": "WHS3_49",
    "hepatitis_b": "HEPATITIS_HBV_INFECTIONS_NEW_NUM"
}

# Create local folder
os.makedirs("data/disease", exist_ok=True)

BASE_URL = "https://ghoapi.azureedge.net/api/"

print("📥 Downloading vaccination data from WHO API...")
def download_disease_data(disease, code):
    url = f"{BASE_URL}{code}?$format=json"
    print(f"📥 Downloading {disease} data from {url}")
    r = requests.get(url)
    if r.status_code == 200:
        records = r.json().get("value", [])
        df = pd.DataFrame(records)
        out_path = f"data/disease/{disease}.csv"
        df.to_csv(out_path, index=False)
        print(f"✅ Saved to {out_path}")
    else:
        print(f"❌ Failed to download {disease} (HTTP {r.status_code})")

if __name__ == "__main__":
    for disease, code in DISEASE_INDICATORS.items():
        download_disease_data(disease, code)
#TODO: Save file locally within data/...
print("📥 Download completed")