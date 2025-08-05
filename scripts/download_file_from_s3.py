#Global function to download files from S3.
#It locates the latest file uploaded and return that one
#Used to avoid repeated code in all files.
import boto3
import re

def download_s3_file(bucket, InputPrefix, FileName):
    s3 = boto3.client("s3")
    """Fetch the most recent file from the forecasting input folder."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=InputPrefix)
    pattern = rf"{re.escape(FileName)}(\d{{8}})\.csv$"
    files = [obj['Key'] for obj in response.get('Contents', []) if re.search(pattern, obj['Key'])]
    latest = max(files, key=lambda x: re.search(r"(\d{8})", x).group(1))
    return latest