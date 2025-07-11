import os
import requests
import pandas as pd

#WHO API Indicators
VACCINE_INDICATORS = {
    "MMR" : "WHS8_110",  
    "DTP3" : "WHS4_100",
    "Pol3" : "WHS4_544",
    "HepB3" : "WHS4_117"
}

#API Base URL
os.makedirs("data/vaccination", exist_ok=True)
BASE = "https://ghoapi.azureedge.net/api/"

#Extracting each vaccine indicator data
print("📥 Downloading vaccination data from WHO API...")
for name, code in VACCINE_INDICATORS.items():
    url = f"{BASE}{code}?$format=json"
    print(f"📥 Fetching {name} ({code})...")
    r = requests.get(url)
    if r.status_code == 200:
        df = pd.DataFrame(r.json().get("value", []))
        df.to_csv(f"data/vaccination/{name}.csv", index=False)
        print(f"✅ Saved to data/vaccination/{name}.csv")
    else:
        print(f"❌ Failed {name}: HTTP {r.status_code}")

print("📥 Download completed")