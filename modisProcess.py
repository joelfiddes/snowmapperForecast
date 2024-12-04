import os
import re
import rasterio
from rasterio.merge import merge
from rasterio import open as rio_open
from rasterio.windows import from_bounds
import pandas as pd
import rasterio
from rasterio import open as rio_open
from rasterio.enums import Resampling
from rasterio.warp import reproject
import numpy as np


# Define the cropping extent (min_x, min_y, max_x, max_y)
extent = (59.475, 32.475, 80.475, 44.475)
pixel_area_km2=0.25
def find_tif_files_by_date_and_type(root_dir):
    """Traverse the directory and group TIFF files by date and type."""
    date_files_map = {
        'snow_cover': {},
        'qa': {}
    }
    
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.tif'):
                # Extract the date part from the filename using regex
                match = re.search(r'A(\d{7})', filename)
                if match:
                    date = match.group(1)  # Get the date part (e.g., 2023277)
                    if '_QA_' in filename:
                        file_type = 'qa'
                    else:
                        file_type = 'snow_cover'

                    if file_type == 'snow_cover':
                        if date not in date_files_map['snow_cover']:
                            date_files_map['snow_cover'][date] = []
                        date_files_map['snow_cover'][date].append(os.path.join(dirpath, filename))
                    else:
                        if date not in date_files_map['qa']:
                            date_files_map['qa'][date] = []
                        date_files_map['qa'][date].append(os.path.join(dirpath, filename))
    
    return date_files_map

def crop_and_merge_tif_files(tif_files, output_file):
    """Crop and merge multiple TIFF files into a single output file."""
    src_files_to_mosaic = []
    
    for tif_file in tif_files:
        with rio_open(tif_file) as src:
            # Calculate the window for cropping
            window = from_bounds(extent[0], extent[1], extent[2], extent[3], transform=src.transform)

            # Read the cropped data
            cropped_data = src.read(window=window)

            # Add the cropped data to the list
            src_files_to_mosaic.append(cropped_data)

    # Merge all the cropped TIFF files
    mosaic, out_trans = merge(src_files_to_mosaic)

    # Get metadata of the first source file
    out_meta = rio_open(tif_files[0]).meta.copy()

    # Update metadata with new dimensions and transform
    out_meta.update({
        'height': mosaic.shape[1],
        'width': mosaic.shape[2],
        'transform': out_trans
    })

    # Write the merged output file
    with rio_open(output_file, 'w', **out_meta) as dest:
        dest.write(mosaic)




def merge_tif_files(tif_files, output_file):
    """Merge multiple TIFF files into a single output file without cropping."""
    src_files_to_mosaic = []

    for tif_file in tif_files:
        src = rio_open(tif_file)
        src_files_to_mosaic.append(src)

    # Merge all the TIFF files
    mosaic, out_trans = merge(src_files_to_mosaic)

    # Get metadata of the first source file
    out_meta = src_files_to_mosaic[0].meta.copy()

    # Update metadata with new dimensions and transform
    out_meta.update({
        'height': mosaic.shape[1],
        'width': mosaic.shape[2],
        'transform': out_trans
    })

    # Write the merged output file
    with rio_open(output_file, 'w', **out_meta) as dest:
        dest.write(mosaic)

    # Close all source files
    for src in src_files_to_mosaic:
        src.close()




def calculate_snow_covered_area(snow_cover_raster, footprint_raster, pixel_area_km2):
    """Calculate the total snow-covered area from the snow cover raster within the footprint."""
    with rio_open(snow_cover_raster) as src_snow:
        snow_data = src_snow.read(1)  # Read the first band of the snow cover
        snow_transform = src_snow.transform
        snow_crs = src_snow.crs

    with rio_open(footprint_raster) as src_footprint:
        # Create an array of zeros with the shape of the snow_data
        footprint_data = np.zeros(snow_data.shape, dtype=src_footprint.dtypes[0])
        
        # Reproject the footprint raster to the snow cover raster's CRS
        reproject(
            src_footprint.read(1),
            destination=footprint_data,
            src_transform=src_footprint.transform,
            src_crs=src_footprint.crs,
            dst_transform=snow_transform,
            dst_crs=snow_crs,
            resampling=Resampling.nearest
        )

    # Create a mask for snow-covered pixels within the footprint
    snow_mask = (snow_data > 0) & (footprint_data > 0)

    # Count the number of valid snow-covered pixels within the footprint
    snow_pixels = snow_mask.sum()

    # Calculate the area in km²
    snow_covered_area_km2 = snow_pixels * pixel_area_km2
    return snow_covered_area_km2




def write_timeseries_to_csv(timeseries_data, output_csv):
    """Write the time series data to a CSV file."""
    df = pd.DataFrame(timeseries_data)
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    # Existing code...

    # Specify the path to the footprint raster
    footprint_raster_path = '/home/joel/Downloads/snow_mask.tif'  # Change this to your footprint raster path

    # Initialize the time series data list at the start
    timeseries_data = []

    # Merge and crop snow cover files
    for date, tif_files in date_files_map['snow_cover'].items():
        print(f"Merging {len(tif_files)} snow cover files for date A{date}.")

        # Create a new directory for the date if it doesn't exist
        date_output_directory = os.path.join(root_directory, date)
        os.makedirs(date_output_directory, exist_ok=True)

        # Specify the output file path for snow cover
        output_file = os.path.join(date_output_directory, f"merged_snow_cover_A{date}.tif")

        # Crop and merge the found snow cover TIFF files
        merge_tif_files(tif_files, output_file)
        print(f"Merged and cropped snow cover file saved as {output_file}.")

        # Calculate snow-covered area within the footprint
        snow_area = calculate_snow_covered_area(output_file, footprint_raster_path, pixel_area_km2)
        timeseries_data.append({'date': f'A{date}', 'snow_covered_area_km2': snow_area})

    # Write the time series data to a CSV file
    output_csv_path = os.path.join(output_directory, 'snow_covered_area_timeseries.csv')
    write_timeseries_to_csv(timeseries_data, output_csv_path)
    print(f"Time series data saved to {output_csv_path}.")

