import requests

def find_codes(search_terms):
    url = "https://ghoapi.azureedge.net/api/Indicator?$format=json"
    items = requests.get(url).json().get("value", [])
    for item in items:
        for term in search_terms:
            if term.lower() in item["IndicatorName"].lower():
                print(f"{item['IndicatorCode']} → {item['IndicatorName']}")

if __name__ == "__main__":
    find_codes(["measles", "diphtheria", "polio", "hepatitis"])