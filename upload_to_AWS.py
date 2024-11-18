import upload as s3
from TopoPyScale import fetch_era5 as fe


SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
aws_access_key_id = "*****"
aws_secret_access_key ="*****"
spatial_directory = "./spatial/"

# Todays era5 file (6days ago)
date_str = fe.return_last_fullday()
formatted_date = date_str.replace('-', '')  

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








# Directory to look for the files


import os
import xarray as xr
import pandas as pd
from datetime import datetime

def bundle_nc_files(directory, formatted_date, file_class, output_file):
    files_on_or_after = []
    formatted_date = datetime.strptime(formatted_date, "%Y%m%d")
    
    # Collect all files on or after the formatted date for the specified class
    for file in os.listdir(directory):
        if file.startswith(file_class) and file.endswith('.nc'):
            file_date_str = file.split('_')[1].replace('.nc', '')
            file_date = datetime.strptime(file_date_str, "%Y%m%d")
            
            if file_date >= formatted_date:
                files_on_or_after.append(file)
    
    # Sort the files by date
    files_on_or_after.sort(key=lambda x: x.split('_')[1].replace('.nc', ''))

    datasets = []
    times = []
    
    # Load each file and extract data along with its time dimension
    for file in files_on_or_after:
        file_path = os.path.join(directory, file)
        ds = xr.open_dataset(file_path)
        
        # Extract the date from the filename and convert it to a pandas datetime object
        file_date_str = file.split('_')[1].replace('.nc', '')
        file_date = pd.to_datetime(file_date_str, format="%Y%m%d")
        times.append(file_date)
        
        # Add the dataset to the list (we will concatenate later)
        datasets.append(ds)
    
    # Concatenate all datasets along a new 'time' dimension
    combined = xr.concat(datasets, dim='time')
    
    # Assign the times to the 'time' dimension
    combined = combined.assign_coords(time=("time", times))
    
    # Save the combined dataset to a new NetCDF file
    combined.to_netcdf(output_file)
    
    print(f"Successfully created {output_file} with time dimension from {file_class} files.")







# Set up
directory = spatial_directory
formatted_date = datetime.now().strftime('%Y%m%d')


# upload SWE
parameter = "SWE"
output_filename_nc = f'SWE_{formatted_date}.nc' # important not written to spatial directory
bundle_nc_files(directory, formatted_date, parameter, output_filename_nc)

output_filename_nc = spatial_directory+f'SWE_{formatted_date}.nc'
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")


# upload HS
parameter = "HS"
output_filename_nc = f'HS_{formatted_date}.nc' # important not written to spatial directory
bundle_nc_files(directory, formatted_date, parameter, output_filename_nc)

output_filename_nc = spatial_directory+f'HS_{formatted_date}.nc'
parameter = "HS"
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")


# upload ROF
parameter = "ROF"
output_filename_nc = f'ROF_{formatted_date}.nc' # important not written to spatial directory
bundle_nc_files(directory, formatted_date, parameter, output_filename_nc)

output_filename_nc = spatial_directory+f'ROF_{formatted_date}.nc'
parameter = "ROF"
s3_path = s3.get_file_path(formatted_date, parameter, True)
success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

if success:
    print(f"{s3_path} File uploaded successfully!")
else:
    print(f"{s3_path} File upload failed.")
