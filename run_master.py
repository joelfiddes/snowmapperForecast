import os
import sys
import shutil
import pandas as pd
from datetime import datetime
from TopoPyScale import topoclass as tc
from munch import DefaultMunch
import xarray as xr
import numpy as np


def load_config(config_file):
    """
    Load configuration from a YAML file.
    
    Parameters:
    - config_file (str): Path to the configuration file.
    
    Returns:
    - config (DefaultMunch): Loaded configuration object.
    """
    try:
        with open(config_file, 'r') as f:
            config = DefaultMunch.fromYAML(f)
        if config.project.directory is None:
            config.project.directory = os.getcwd() + '/'
        return config
    except IOError:
        raise FileNotFoundError(f"ERROR: config file does not exist.\n\t Current file path: {config_file}\n\t Current working directory: {os.getcwd()}")

def parse_filename(file_path):
    """
    Parse the filename to extract start and end dates.
    
    Parameters:
    - file_path (str): Path to the file.
    
    Returns:
    - start_date (pd.Timestamp): Start date extracted from the filename.
    - end_date (pd.Timestamp): End date calculated from the start date.
    """
    filename = os.path.basename(file_path)
    year, month = int(filename.split('_')[1][:4]), int(filename.split('_')[1][4:6])
    start_date = pd.Timestamp(year, month, 1, 0)
    end_date = start_date + pd.offsets.MonthEnd(0) + pd.DateOffset(hours=23)
    return start_date, end_date

def check_timesteps(file_path, error_files):
    """
    Check if all model timesteps are present in a file.
    
    Parameters:
    - file_path (str): Path to the file to check.
    - error_files (list): List to append files with missing timesteps.
    """
    try:
        ds = xr.open_dataset(file_path)
        actual_time_steps = pd.to_datetime(ds.time.values)
        start_date, end_date = parse_filename(file_path)
        expected_time_steps = pd.date_range(start=start_date, end=end_date, freq='1H')
        missing_time_steps = expected_time_steps[~expected_time_steps.isin(actual_time_steps)]
        if missing_time_steps.empty:
            print(f"All model timesteps are present in {file_path}.")
        else:
            print(f"Missing time steps in {file_path}: {missing_time_steps}")
            error_files.append(file_path)
    except (FileNotFoundError, KeyError) as e:
        print(f"Error processing {file_path}: {str(e)}")
        error_files.append(file_path)

def delete_files(file_paths):
    """
    Delete files listed in the provided file_paths.
    
    Parameters:
    - file_paths (list): A list of file paths to be deleted.
    """
    for file_path in file_paths:
        try:
            os.remove(file_path)
            print(f"File deleted: {file_path}")
        except Exception as e:
            print(f"Error deleting file or file doesn't exist {file_path}: {str(e)}")

def generate_file_paths(start_year, end_year, end_month, file_types):
    """
    Generate file paths for a given range of months and file types.
    
    Parameters:
    - start_year (int): Start year for generating file paths.
    - end_year (int): End year for generating file paths.
    - end_month (int): End month for generating file paths.
    - file_types (list): List of file types to generate paths for.
    
    Returns:
    - file_paths (list): List of generated file paths.
    """
    start_date = pd.Timestamp(start_year, 9, 1, 0)  # September of the start year
    end_date = pd.Timestamp(end_year, end_month, 1, 0) + pd.offsets.MonthEnd(0)
    time_vector = pd.date_range(start=start_date, end=end_date, freq='1M')
    return [f"{file_type}_{time.strftime('%Y%m')}.nc" for time in time_vector for file_type in file_types]

def trim_forecast_data(climate_file, forecast_file, output_file):
    """
    Trim forecast data to remove overlapping time steps with climate data.
    
    Parameters:
    - climate_file (str): Path to the climate data file.
    - forecast_file (str): Path to the forecast data file.
    - output_file (str): Path to save the trimmed forecast data.
    """
    ds1 = xr.open_dataset(climate_file)
    ds2 = xr.open_dataset(forecast_file)
    last_time_file1 = ds1.time[-1].values
    next_time = last_time_file1 + np.timedelta64(1, 'h')
    trimmed_ds2 = ds2.sel(time=slice(next_time, None))
    trimmed_ds2.to_netcdf(output_file)
    print(f"Trimmed forecast data saved to {output_file}")





def trim_forecast_data2(climate_file, forecast_file, output_file):
    """
    Trim forecast data to remove overlapping time steps with climate data.

    Ensures the climate data ends at 23:00 and the forecast data starts at 00:00.

    Parameters:
    - climate_file (str): Path to the climate data file.
    - forecast_file (str): Path to the forecast data file.
    - output_file (str): Path to save the trimmed forecast data.
    """
    try:
        # Open datasets
        ds1 = xr.open_dataset(climate_file)
        ds2 = xr.open_dataset(forecast_file)

        # Get the last time in the climate data
        last_time_file1 = ds1.time[-1].values

        # Ensure climate data ends at 23:00
        # Convert last_time to a datetime and check the hour
        if np.datetime64(last_time_file1).astype('datetime64[h]').astype(str)[-2:] != '23':
            print(f"Trimming climate data to end at 23:00.")
            # Trim the climate data to end at the last occurrence of 23:00
            ds1 = ds1.sel(time=slice(None, str(last_time_file1)[:10] + 'T23:00:00'))
            last_time_file1 = ds1.time[-1].values  # Update last time after trimming

        # Calculate the next time step (start of forecast data), which should be 00:00 of the next day
        next_time = (last_time_file1 + np.timedelta64(1, 'h')).astype('datetime64[D]') + np.timedelta64(0, 'h')

        # Trim forecast data to start at 00:00 of the next day
        trimmed_ds2 = ds2.sel(time=slice(next_time, None))

        # Save the trimmed forecast data
        trimmed_ds2.to_netcdf(output_file)
        print(f"Trimmed forecast data saved to {output_file}")

    except KeyError as e:
        print(f"Error: Time coordinate not found in one of the datasets. {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close datasets
        ds1.close()
        ds2.close()


def main():
    start_time = datetime.now()
    mydir = sys.argv[1]
    os.chdir(mydir)

    config_file = './config.yml'
    config = load_config(config_file)
    start_year = config.project.start.year

    current_month = pd.Timestamp.now().month
    current_year = pd.Timestamp.now().year

    file_types = ['./inputs/climate/SURF', './inputs/climate/PLEV']
    file_paths = generate_file_paths(start_year, current_year, current_month, file_types)

    error_files = []
    for file_path in file_paths:
        check_timesteps(file_path, error_files)
    
    if error_files:
        print("Files with errors now deleted:", error_files)
        delete_files(error_files)
    
    # Initialize Topoclass and perform operations
    mp = tc.Topoclass(config_file)

    # download latest climate data
    mp.get_era5()
    mp.remap_netcdf()
    
#    # Trim forecast data to make sure no overlap with latest download month
#    latest_nc_file_surf = f'./inputs/climate/SURF_{current_year:04d}{current_month:02d}.nc'
#    latest_nc_file_plev = f'./inputs/climate/PLEV_{current_year:04d}{current_month:02d}.nc'
#    trim_forecast_data2(latest_nc_file_surf, './inputs/climate/forecast/SURF_fc_cat.nc', './inputs/climate/forecast/SURF_fc.nc')
#    trim_forecast_data2(latest_nc_file_plev, './inputs/climate/forecast/PLEV_fc_cat.nc', './inputs/climate/forecast/PLEV_fc.nc')

#    os.remove('./inputs/climate/forecast/SURF_fc_cat.nc')
#    os.remove('./inputs/climate/forecast/PLEV_fc_cat.nc')

    print(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
