# https://forum.ecmwf.int/t/forthcoming-update-to-the-format-of-netcdf-files-produced-by-the-conversion-of-grib-data-on-the-cds/7772


import os
import zipfile
import xarray as xr
import shutil
import argparse

def process_file(file_path, workdir):
    original_file_path = file_path  # Keep track of the original file name
    
    # Step 1: Try to open as a NetCDF file
    try:
        with xr.open_dataset(file_path) as ds:
            print(f"{file_path} is a valid NetCDF file. No processing needed.")
            return
    except Exception:
        print(f"{file_path} is not a valid NetCDF file. Checking if it's a ZIP file.")
    
    # Step 2: Check if it's a ZIP file
    if not zipfile.is_zipfile(file_path):
        print(f"{file_path} is neither a valid NetCDF nor a ZIP file.")
        return
    
    # Step 3: Rename the file if it's actually a ZIP
    zip_file_path = file_path.replace('.nc', '.zip')
    os.rename(file_path, zip_file_path)
    print(f"Renamed {file_path} to {zip_file_path} for processing.")
    
    # Step 4: Unzip the file
    unzip_dir = zip_file_path.replace('.zip', '')
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    print(f"Unzipped {zip_file_path} to {unzip_dir}.")
    
    # Step 5: Merge `.nc` files inside the unzipped directory
    nc_files = [os.path.join(unzip_dir, f) for f in os.listdir(unzip_dir) if f.endswith('.nc')]
    if not nc_files:
        print(f"No .nc files found in {unzip_dir}.")
        return
    
    merged_file_path = os.path.join(workdir, os.path.basename(zip_file_path).replace('.zip', '.nc'))
    try:
        # Combine all `.nc` files
        datasets = [xr.open_dataset(nc_file) for nc_file in nc_files]
        merged_ds = xr.concat(datasets, dim='time')  # Adjust dimension as needed
        merged_ds.to_netcdf(merged_file_path)
        print(f"Merged .nc files into {merged_file_path}.")
    finally:
        # Close datasets
        for ds in datasets:
            ds.close()
    
    # Step 6: Clean up
    os.remove(zip_file_path)
    shutil.rmtree(unzip_dir)
    print(f"Deleted {zip_file_path} and {unzip_dir}.")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process a misidentified NetCDF or ZIP file.")
    parser.add_argument("file_path", help="Path to the input file (NetCDF or ZIP).")
    parser.add_argument("workdir", help="Path to the working directory where output should be saved.")
    args = parser.parse_args()
    
    # Call the function with the provided arguments
    process_file(args.file_path, args.workdir)
    
    # Example usage
    # file_path = "SURF_20241128.nc"  # Replace with your file path
    # workdir = "/path/to/workdir"  # Replace with your working directory
    # process_file(file_path, workdir)

