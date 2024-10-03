import upload as s3
mydir = sys.argv[1]

SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
aws_access_key_id = "AKIAROB2RVVCHEK5X3R2"
aws_secret_access_key ="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"
spatial_directory = mydir + "/spatial/"



# Todays file (6days ago)
formatted_date



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




# forecast file
formatted_date # date of forecast start

output_filename_nc = spatial_directory+f'SWE_{formatted_date}.nc'
parameter = "SWE"
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")

output_filename_nc = spatial_directory+f'HS_{formatted_date}.nc'
parameter = "HS"
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")


output_filename_nc = spatial_directory+f'ROF_{formatted_date}.nc'
parameter = "ROF"
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")
