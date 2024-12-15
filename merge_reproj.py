import xarray as xr
import rioxarray  # Import the rioxarray module for raster I/O operations
from datetime import datetime
import pyproj
import os
import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling
import numpy as np
import sys
import upload as s3
import glob

mydir = sys.argv[1]
upload_to_aws = sys.argv[2]
print("upload_to_aws =" + upload_to_aws)
SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
PARAMETERS = ["HS", "SWE"]
aws_access_key_id = "xxxx"
aws_secret_access_key ="xxxx"

#year= sys.argv[1]
startTime = datetime.now()
thismonth = startTime.month    # now
thisyear = startTime.year 
year = [thisyear if thismonth in {9, 10, 11, 12} else thisyear-1][0]
fileyear = year+1

print("Water year: " + str(fileyear))

# Define the target projection as longitude and latitude

# Define the target projection as longitude and latitude
target_projection = 'EPSG:4326'  # EPSG code for WGS 84 coordinate system (longitude and latitude)



#========== SWE computation ====================================================

# Directory containing merged reprojected files
spatial_directory = "./spatial/"
os.makedirs(spatial_directory, exist_ok=True) 
# Loop over years
 # Adjust the range according to your desired years
# Load the NetCDF files for each year

# file1 = f"./myproject/D1/outputs/*_SWE.nc"
# file2 = f"./myproject/D2/outputs/*_SWE.nc"
# file3 = f"./myproject/D3/outputs/*_SWE.nc"
variable_name ="SWE"
filep1 =    f"{mydir}/D1/outputs/*_{variable_name}.nc"
file1 = glob.glob(filep1)
filep2 =    f"{mydir}/D2/outputs/*_{variable_name}.nc"
file2 = glob.glob(filep2)
filep3 =    f"{mydir}/D3/outputs/*_{variable_name}.nc"
file3 = glob.glob(filep3)

# Open the datasets
ds1 = xr.open_dataset(file1[0])
ds2 = xr.open_dataset(file2[0])
ds3 = xr.open_dataset(file3[0])

# Loop through each timestep
for time_idx, time_value in enumerate(ds1.Time.values):

    formatted_date = np.datetime_as_string(time_value, unit='D')
    # Now format as YYYYMMDD
    formatted_date = formatted_date.replace('-', '')
    print(formatted_date)

    # Construct output filename
    output_filename_nc = spatial_directory+f'SWE_{formatted_date}.nc'
    output_filename_tif = spatial_directory+f'swe_merged_reprojected_{year}_{time_idx}.tif'
    
    if os.path.exists(output_filename_tif):
        print(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Select data for the current timestep
    ds1_slice = ds1.isel(Time=time_idx)
    ds2_slice = ds2.isel(Time=time_idx)
    ds3_slice = ds3.isel(Time=time_idx)

    # Set spatial dimensions
    ds1_slice = ds1_slice.rename({'easting': 'x','northing': 'y'})
    ds2_slice = ds2_slice.rename({'easting': 'x','northing': 'y'})
    ds3_slice = ds3_slice.rename({'easting': 'x','northing': 'y'})
    ds1_slice = ds1_slice.rio.write_crs(pyproj.CRS.from_epsg(32643).to_wkt())
    ds2_slice = ds2_slice.rio.write_crs(pyproj.CRS.from_epsg(32642).to_wkt())
    ds3_slice = ds3_slice.rio.write_crs(pyproj.CRS.from_epsg(32641).to_wkt())

    # Reproject ds1 to latitude and longitude
    ds1_latlon = ds1_slice.rio.reproject(target_projection)

    # Reproject ds2 to latitude and longitude
    ds2_latlon = ds2_slice.rio.reproject(target_projection)
    
    # Reproject ds2 to latitude and longitude
    ds3_latlon = ds3_slice.rio.reproject(target_projection)
    
    # Define output filenames
    output_filename_ds1 = f'ds1_reprojected_{year}_{time_idx}.tif'
    output_filename_ds2 = f'ds2_reprojected_{year}_{time_idx}.tif'
    output_filename_ds3 = f'ds3_reprojected_{year}_{time_idx}.tif'
    
    # Write reprojected datasets to GeoTIFF files
    ds1_latlon.rio.to_raster(output_filename_ds1)
    ds2_latlon.rio.to_raster(output_filename_ds2)
    ds3_latlon.rio.to_raster(output_filename_ds3)
    
    # Function to extract year and date from filename
    def extract_year_date(filename):
        year = filename.split('_')[2]
        date = filename.split('_')[3].split('.')[0]
        return year, date

    # List of filenames of GeoTIFF files to merge
    file_list = [output_filename_ds1, output_filename_ds2, output_filename_ds3]  # Add more filenames as needed

    # Extract year and date from the first input filename
    year, date = extract_year_date(file_list[0])

    # Open each GeoTIFF file
    src_files_to_mosaic = [rasterio.open(file) for file in file_list]

    # Merge the raster datasets
    mosaic, out_trans = merge(src_files_to_mosaic, resampling=Resampling.cubic)


    
    if os.path.exists(output_filename_tif):
        print(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Write the merged raster to a new GeoTIFF file
    with rasterio.open(output_filename_tif, 'w', driver='GTiff', 
                       width=mosaic.shape[2], height=mosaic.shape[1], 
                       count=mosaic.shape[0], dtype=mosaic.dtype,
                       crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)
        

    from netCDF4 import Dataset, default_fillvals
    # Define the variable name and other attributes
    variable_name = 'Band1'
    new_variable_name = 'swe'
    variable_long_name = 'snow_water_equivalent'
    variable_units = 'mm'
    resolution_m = '500'  # resolution in meters

    # Create the NetCDF file
    with rasterio.open(output_filename_nc, 'w', driver='NetCDF', 
                   width=mosaic.shape[2], height=mosaic.shape[1], count=mosaic.shape[0], 
                   dtype=mosaic.dtype, nodata=-9999, 
                   crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)
        
    # Open the NetCDF file again with netCDF4 to set attributes
    with Dataset(output_filename_nc, 'r+') as ds:
    
        # Rename the variable
        ds.renameVariable(variable_name, new_variable_name)
        
        # Set variable attributes
        ds[new_variable_name].setncatts({
             'long_name': variable_long_name,
            'units': variable_units,
            'resolution_m': resolution_m
        })


    # Delete input raster files
    for file in file_list:
        os.remove(file)


    if upload_to_aws:
        parameter = "SWE"
        s3_path = s3.get_file_path(formatted_date, parameter)
        success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

        if success:
            print(f"{variable_name} File uploaded successfully!")
        else:
            print(f"{variable_name} File upload failed.")

#========== HS computation ====================================================


# Loop over years
 # Adjust the range according to your desired years
# Load the NetCDF files for each year

variable_name ="HS"
filep1 =    f"{mydir}/D1/outputs/*_{variable_name}.nc"
file1 = glob.glob(filep1)
filep2 =    f"{mydir}/D2/outputs/*_{variable_name}.nc"
file2 = glob.glob(filep2)
filep3 =    f"{mydir}/D3/outputs/*_{variable_name}.nc"
file3 = glob.glob(filep3)

# Open the datasets
ds1 = xr.open_dataset(file1[0])
ds2 = xr.open_dataset(file2[0])
ds3 = xr.open_dataset(file3[0])

# Loop through each timestep
for time_idx, time_value in enumerate(ds1.Time.values):

    formatted_date = np.datetime_as_string(time_value, unit='D')
    # Now format as YYYYMMDD
    formatted_date = formatted_date.replace('-', '')
    print(formatted_date)

    # Construct output filename
    output_filename_nc = spatial_directory+f'HS_{formatted_date}.nc'
    output_filename_tif = spatial_directory+f'hs_merged_reprojected_{year}_{time_idx}.tif'
    
    if os.path.exists(output_filename_tif):
        print(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Select data for the current timestep
    ds1_slice = ds1.isel(Time=time_idx)
    ds2_slice = ds2.isel(Time=time_idx)
    ds3_slice = ds3.isel(Time=time_idx)

    # Set spatial dimensions
    ds1_slice = ds1_slice.rename({'easting': 'x','northing': 'y'})
    ds2_slice = ds2_slice.rename({'easting': 'x','northing': 'y'})
    ds3_slice = ds3_slice.rename({'easting': 'x','northing': 'y'})
    ds1_slice = ds1_slice.rio.write_crs(pyproj.CRS.from_epsg(32643).to_wkt())
    ds2_slice = ds2_slice.rio.write_crs(pyproj.CRS.from_epsg(32642).to_wkt())
    ds3_slice = ds3_slice.rio.write_crs(pyproj.CRS.from_epsg(32641).to_wkt())

    # Reproject ds1 to latitude and longitude
    ds1_latlon = ds1_slice.rio.reproject(target_projection)

    # Reproject ds2 to latitude and longitude
    ds2_latlon = ds2_slice.rio.reproject(target_projection)
    
    # Reproject ds2 to latitude and longitude
    ds3_latlon = ds3_slice.rio.reproject(target_projection)
    
    # Define output filenames
    output_filename_ds1 = f'ds1_reprojected_{year}_{time_idx}.tif'
    output_filename_ds2 = f'ds2_reprojected_{year}_{time_idx}.tif'
    output_filename_ds3 = f'ds3_reprojected_{year}_{time_idx}.tif'
    
    # Write reprojected datasets to GeoTIFF files
    ds1_latlon.rio.to_raster(output_filename_ds1)
    ds2_latlon.rio.to_raster(output_filename_ds2)
    ds3_latlon.rio.to_raster(output_filename_ds3)
    
    # Function to extract year and date from filename
    def extract_year_date(filename):
        year = filename.split('_')[2]
        date = filename.split('_')[3].split('.')[0]
        return year, date

    # List of filenames of GeoTIFF files to merge
    file_list = [output_filename_ds1, output_filename_ds2, output_filename_ds3]  # Add more filenames as needed

    # Extract year and date from the first input filename
    year, date = extract_year_date(file_list[0])

    # Open each GeoTIFF file
    src_files_to_mosaic = [rasterio.open(file) for file in file_list]

    # Merge the raster datasets
    mosaic, out_trans = merge(src_files_to_mosaic, resampling=Resampling.average)


    
    if os.path.exists(output_filename_tif):
        print(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Write the merged raster to a new GeoTIFF file
    with rasterio.open(output_filename_tif, 'w', driver='GTiff', 
                       width=mosaic.shape[2], height=mosaic.shape[1], 
                       count=mosaic.shape[0], dtype=mosaic.dtype,
                       crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)
        
        
    from netCDF4 import Dataset, default_fillvals
    # Define the variable name and other attributes
    variable_name = 'Band1'
    new_variable_name = 'hs'
    variable_long_name = 'snow_height'
    variable_units = 'm'
    resolution_m = '500'  # resolution in meters

    # Create the NetCDF file
    with rasterio.open(output_filename_nc, 'w', driver='NetCDF', 
                   width=mosaic.shape[2], height=mosaic.shape[1], count=mosaic.shape[0], 
                   dtype=mosaic.dtype, nodata=-9999, 
                   crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)
        
    # Open the NetCDF file again with netCDF4 to set attributes
    with Dataset(output_filename_nc, 'r+') as ds:
    
        # Rename the variable
        ds.renameVariable(variable_name, new_variable_name)
        
        # Set variable attributes
        ds[new_variable_name].setncatts({
             'long_name': variable_long_name,
            'units': variable_units,
            'resolution_m': resolution_m
        })

    # Optionally, visualize the merged raster
    # with rasterio.open(output_filename) as src:
        # show(src)

    # Delete input raster files
    for file in file_list:
        os.remove(file)
        


    if upload_to_aws:
        parameter = "HS"
        s3_path = s3.get_file_path(formatted_date, parameter)
        success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path, aws_access_key_id, aws_secret_access_key)

        if success:
            print(f"{variable_name} File uploaded successfully!")
        else:
            print(f"{variable_name} File upload failed.")


        
        
        
