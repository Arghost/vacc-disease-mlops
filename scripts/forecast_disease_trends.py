#File to generate forecasts using multiple method
import boto3
import re
import pandas as pd
import numpy as np
import io
from statsmodels.tsa.api import SARIMAX, ExponentialSmoothing
from sklearn.metrics import mean_absolute_percentage_error as mape
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor
from datetime import datetime
from datetime import timezone as timz
import warnings

s3 = boto3.client("s3")
S3_BUCKET = "vacc-disease-mlops-pipeline-argh"
INPUT_PREFIX = "processed/forecasting/"
InputFileName = "cleaned_for_forecast_"
warnings.filterwarnings("ignore")

#Function to download S3 files.
def download_s3_file():
    s3 = boto3.client("s3")
    latest = ""
    """Fetch the most recent file from the forecasting input folder."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=INPUT_PREFIX)
    pattern = rf"{re.escape(InputFileName)}(\d{{8}})\.csv$"
    files = [obj['Key'] for obj in response.get('Contents', []) if re.search(pattern, obj['Key'])]
    latest = max(files, key=lambda x: re.search(r"(\d{8})", x).group(1))
    return latest

#Main method to group data and generate forecasting models.
def generate_forecast_models(df):
    results = []
    df_dis = df[df["type"] == "Disease"]
    print("Starting with forecasting process")
    for (country, disease), group in df_dis.groupby(["country", "disease_name"]):
        ts = group.sort_values("year")[["year", "value"]].dropna()
        if len(ts) < 5:
            continue
        X = ts["year"].values.reshape(-1, 1)
        y = ts["value"].values
        future_years = np.array(range(ts["year"].max() + 1, ts["year"].max() + 6)).reshape(-1, 1)
        forecasts = {}
        scores = {}

        #1. ARIMA        
        try:
            model = SARIMAX (y, order=(1,1,1), seasonal_order=(0,0,0,0))
            arima_fit = model.fit(disp=False)
            forecast = arima_fit.forecast(steps=5)
            forecasts["ARIMA"] = forecast
            scores["ARIMA"] = mape(y[-5:], arima_fit.predict(start=len(y)-5, end=len(y)-1))
        except Exception as e:
            pass

        #2. ETS
        try:
            ets = ExponentialSmoothing(y, trend='add', seasonal=None).fit()
            forecast = ets.forecast(5)
            forecasts["ETS"] = forecast
            scores["ETS"] = mape(y[-5:], ets.predict(start=len(y)-5, end=len(y)-1))
        except Exception as e:
            pass

        # 3. Linear Regression
        try:
            lr = LinearRegression().fit(X, y)
            forecast = lr.predict(future_years)
            forecasts["LinearRegression"] = forecast
            scores["LinearRegression"] = mape(y, lr.predict(X))
        except Exception as e:
            pass
        
        # 4. XGBoost
        try:
            xgb = XGBRegressor(n_estimators=100)
            xgb.fit(X, y)
            forecast = xgb.predict(future_years)
            forecasts["XGBoost"] = forecast
            scores["XGBoost"] = mape(y, xgb.predict(X))
        except Exception as e:
            pass

        # Choose best
        if scores:
            best_model = min(scores, key=scores.get)
            best_forecast = forecasts[best_model]
            for i, year in enumerate(future_years.flatten()):
                results.append({
                    "country": country,
                    "disease": disease,
                    "year": int(year),
                    "forecast": float(best_forecast[i]),
                    "model": best_model
                })

    print("Forecasting process complete.")
    # Upload results
    df_result = pd.DataFrame(results)
    csv_buffer = io.StringIO()
    df_result.to_csv(csv_buffer, index=False)
    # Create timestamp
    tmstamp = datetime.now(tz=timz.utc).strftime("%Y%m%d")
    output_key = f"processed/forecast/forecasted_data_{tmstamp}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=csv_buffer.getvalue())
    print(f"✅ Forecasts saved to S3 → {output_key}")

    return

#Main method to execute forecast
def execute_forecast(key):
    """Download a CSV file from S3 into a DataFrame."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_csv(response['Body'])
    if (df.empty == True):
        return {
        "statusCode": 404,
        "body": "❌ File for forecast models not found"
    }
    else:
        print(f"✅ Data loaded from S3 with " + str(len(df)) + " records")
        generate_forecast_models(df)
    return

#lambda handler for AWS
def statsmodels_layer():
    latest_key = download_s3_file ()
    if (latest_key != ""):
        print(f"📥 Latest input: {latest_key}")
        execute_forecast(latest_key)
    else: 
        return {
        "statusCode": 404,
        "body": "❌ File for forecast models not found"
    }    
    return {
        "statusCode": 200,
        "body": "✅ Forecast models generated"
    }

if __name__ == "__main__":
    statsmodels_layer()