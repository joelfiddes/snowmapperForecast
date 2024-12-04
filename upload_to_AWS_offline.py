import upload as s3
from TopoPyScale import fetch_era5 as fe
import boto3
from datetime import date
import sys
# python /home/ubuntu/src/snowmapperForecast/upload_to_AWS_offline.py "20241120"
formatted_date = sys.argv[1]
# date = "2024-11-07"


# Use default profile credentials
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
aws_access_key_id = credentials.access_key
aws_secret_access_key = credentials.secret_key
spatial_directory = "./spatial/"

output_filename_nc = spatial_directory+f'SWE_{formatted_date}.nc'
parameter = "SWE"
s3_path = s3.get_file_path(formatted_date, parameter)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")

output_filename_nc = spatial_directory+f'HS_{formatted_date}.nc'
parameter = "HS"
s3_path = s3.get_file_path(formatted_date, parameter)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")

output_filename_nc = spatial_directory+f'ROF_{formatted_date}.nc'
parameter = "ROF"
s3_path = s3.get_file_path(formatted_date, parameter)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")




