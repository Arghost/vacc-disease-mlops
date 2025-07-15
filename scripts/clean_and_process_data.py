import json
import boto3

# Function to clean and process data
def clean_and_process_data():



# Lambda handler for data cleaning and processing
def lambda_handler(event=None, context=None):
        #intialize S3 client
    s3 = boto3.client("s3")
    bucket_name = "vacc-disease-mlops-pipeline-argh"
    print("🚀 Starting data cleaning and processing...")
    try:
        # List objects in the S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw/")
        if 'Contents' not in response:
            print("❌ No raw data found in the bucket.")
            return {
                "statusCode": 404,
                "body": json.dumps("No raw data found.")
            }

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith(".csv"):
                print(f"🔄 Processing {key}...")
                # Here you would add your data cleaning and processing logic
                # For now, we just simulate processing
                print(f"✅ Processed {key}")

        print("✅ All data cleaned and processed successfully.")
        return {
            "statusCode": 200,
            "body": json.dumps("Data cleaning and processing complete.")
        }
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }

if __name__ == "__main__":
    lambda_handler()
    print("🚀 Ingestion process completed.")