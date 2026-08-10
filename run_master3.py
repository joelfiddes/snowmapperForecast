# redone merge routine to make more rubust

import os
import sys
import shutil
import pandas as pd
from datetime import datetime, timedelta
from TopoPyScale import topoclass as tc
from munch import DefaultMunch
import xarray as xr
import numpy as np
import concurrent.futures
import glob



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



def merge_datasets(pattern1, pattern2, output_path):
    """
    Load datasets from specified patterns, interpolate, merge, and save to a new NetCDF file.
    
    Parameters:
        pattern1 (str): File pattern for the first group of datasets.
        pattern2 (str): File pattern for the second group of datasets.
        output_path (str): Path to save the merged dataset.
        
    Returns:
        xarray.Dataset: The merged dataset.
    """
    # Find files that match the patterns
    grid1_files = glob.glob(pattern1)
    grid2_files = glob.glob(pattern2)

    # Load the files from both grids
    ds_grid1_list = [xr.open_dataset(file) for file in grid1_files]
    ds_grid2_list = [xr.open_dataset(file) for file in grid2_files]

    # Choose the grid from the first file in Grid 1 as the common grid
    common_grid = ds_grid1_list[0]  # Assuming all grid 1 files share the same grid

    # Interpolate all Grid 2 files to the common grid (Grid 1)
    ds_grid2_interp_list = [ds.interp(latitude=common_grid.latitude, longitude=common_grid.longitude) for ds in ds_grid2_list]

    # Merge all Grid 1 files into a single dataset
    ds_grid1 = xr.merge(ds_grid1_list)

    # Merge all Grid 2 (interpolated) files into a single dataset
    ds_grid2_interp = xr.merge(ds_grid2_interp_list)

    # Now merge both datasets into a single one
    ds_merged = xr.merge([ds_grid1, ds_grid2_interp])

    # Save the merged dataset to a new NetCDF file
    ds_merged.to_netcdf(output_path)

    print(f"All files merged and saved to {output_path}")
    #return ds_merged


def merge_datasets_filter(pattern1, pattern2, output_path):
    """
    Load datasets from specified patterns, interpolate, merge, and save to a new NetCDF file.
    
    Parameters:
        pattern1 (str): File pattern for the first group of datasets.
        pattern2 (str): File pattern for the second group of datasets.
        output_path (str): Path to save the merged dataset.
        
    Returns:
        xarray.Dataset: The merged dataset.
    """
    # Get today's date and calculate the cutoff date (9 days ago)
    cutoff_date = datetime.now() - timedelta(days=9)

    # Find files that match the patterns
    grid1_files = glob.glob(pattern1)
    grid2_files = glob.glob(pattern2)
    
    # Check if files were found
    if not grid1_files:
        raise FileNotFoundError(f"No files found for pattern: {pattern1}")
    if not grid2_files:
        raise FileNotFoundError(f"No files found for pattern: {pattern2}")
    
    # Filter grid1_files by date (extract the date from the filename)
    filtered_grid1_files = []
    for file in grid1_files:
        # Extract date from the filename assuming format "SURF_YYYYMMDD.nc"
        filename = os.path.basename(file)
        date_str = filename.split('_')[1].split('.')[0]  # Extract "YYYYMMDD"
        
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
            # Only keep files that are after the cutoff date
            if file_date >= cutoff_date:
                filtered_grid1_files.append(file)
        except ValueError:
            print(f"Could not parse date from filename: {filename}")

    # Check if any files remain after filtering
    if not filtered_grid1_files:
        raise FileNotFoundError("No grid1 files found within the last 9 days.")
    
    # Load the files from both grids (use lazy loading for efficiency if datasets are large)
    ds_grid1_list = [xr.open_dataset(file) for file in filtered_grid1_files]
    ds_grid2_list = [xr.open_dataset(file) for file in grid2_files]
    
    # Choose the grid from the first file in Grid 1 as the common grid
    common_grid = ds_grid1_list[0]  # Assuming all grid 1 files share the same grid
    
    # Interpolate all Grid 2 files to the common grid (Grid 1)
    ds_grid2_interp_list = [
        ds.interp(latitude=common_grid.latitude, longitude=common_grid.longitude)
        for ds in ds_grid2_list
    ]
    
    # Merge all Grid 1 files into a single dataset
    ds_grid1 = xr.merge(ds_grid1_list)
    
    # Merge all Grid 2 (interpolated) files into a single dataset
    ds_grid2_interp = xr.merge(ds_grid2_interp_list)
    
    # Now merge both datasets into a single one
    ds_merged = xr.merge([ds_grid1, ds_grid2_interp]) # override”: skip comparing and pick variable from first dataset ie era5 gets prioritised if overlap exists
    
    # Save the merged dataset to a new NetCDF file
    ds_merged.to_netcdf(output_path)
    
    print(f"All files merged and saved to {output_path}")
    
    #return ds_merged




def merge_forecast_with_merged(ds_merged_path, ds_surf_fc_path, output_path):
    """
    Merge the ERA5 gapfill and forecast dataset with the previously merged dataset.
    
    Parameters:
        ds_merged_path (str): Path to the merged dataset file.
        ds_surf_fc_path (str): Path to the surf forecast dataset file.
        output_path (str): Path to save the final merged dataset.
    """
    # Load the datasets
    ds_merged = xr.open_dataset(ds_merged_path)
    ds_surf_fc = xr.open_dataset(ds_surf_fc_path)

    # Interpolate ds_surf_fc to match the grid of ds_merged
    ds_surf_fc_interp = ds_surf_fc.interp(latitude=ds_merged.latitude, longitude=ds_merged.longitude)

    # Check for overlapping time
    overlap_start = max(ds_merged.time.min(), ds_surf_fc_interp.time.min())
    overlap_end = min(ds_merged.time.max(), ds_surf_fc_interp.time.max())

    # Check if there is an overlap
    if overlap_start < overlap_end:
        print(f"Overlap detected from {overlap_start.values} to {overlap_end.values}")
        # Dynamically slice ds_merged to remove the overlapping time
        ds_merged_cleaned = ds_merged.sel(time=slice(None, overlap_start - pd.Timedelta(hours=1)))
    else:
        print("No overlapping time detected.")
        ds_merged_cleaned = ds_merged  # No need to slice if there's no overlap

    # Concatenate the datasets along the 'time' dimension
    ds_final = xr.concat([ds_merged_cleaned, ds_surf_fc_interp], dim='time')

    # Save the final merged dataset
    ds_final.to_netcdf(output_path)

    print(f"Final merged dataset saved to {output_path}")


def convert_time_units_to_ncview_compatible(dataset_path, output_path):
    """
    Convert the time units of the dataset to 'hours since' the start of the dataset.
    
    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
        output_path (str): Path to save the modified dataset.
        # Example usage
    dataset_path = '/home/joel/sim/snowmapper_2025/master/inputs/climate/SURF_final_merged_output.nc'
    output_path = '/home/joel/sim/snowmapper_2025/master/inputs/climate/SURF_fixed_time_output.nc'
    convert_time_units_to_ncview_compatible(dataset_path, output_path)
    """
    try:
        # Load the dataset
        ds = xr.open_dataset(dataset_path)

        # Ensure 'time' variable exists in the dataset
        if 'time' not in ds:
            raise ValueError("'time' variable not found in the dataset.")

        # Convert time units to 'hours since' the start of the dataset
        start_time = pd.to_datetime(ds['time'].values[0])  # Get the first time point

        # Convert time to hours since start_time
        ds['time'] = (ds['time'] - ds['time'][0]).astype('timedelta64[h]')

        # Handle encoding instead of directly modifying attributes
        ds['time'].encoding['units'] = f"hours since {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        ds['time'].encoding['calendar'] = 'proleptic_gregorian'
        
        # Set standard and long names directly in the attributes
        ds['time'].attrs['standard_name'] = 'time'
        ds['time'].attrs['long_name'] = 'time'

        # Save the dataset with the corrected time variable
        ds.to_netcdf(output_path)

        print(f"Saved file with corrected time to {output_path}")
    
    except Exception as e:
        print(f"Error: {e}")





def check_duplicate_and_missing_times(dataset_path):
    """
    Check for duplicate and missing timestamps in the dataset.

    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
    """
    # Load the dataset
    ds = xr.open_dataset(dataset_path)

    # Convert ds.time to a pandas DatetimeIndex
    mytimeseries = pd.to_datetime(ds.time.values)

    # Create a complete time range from the start to the end of mytimeseries with an hourly frequency
    complete_time_range = pd.date_range(start=mytimeseries.min(), end=mytimeseries.max(), freq='H')

    # Find missing times by comparing the two time series
    missing_times = complete_time_range.difference(mytimeseries)

    # Check for duplicate times
    duplicate_times = mytimeseries[mytimeseries.duplicated()]

    # Print the missing times, if any
    if not missing_times.empty:
        print("Missing times found:")
        print(missing_times)
    else:
        print("No missing times in the time series.")

    # Print the duplicate times, if any
    if not duplicate_times.empty:
        print("Duplicate times found:")
        print(duplicate_times)
    else:
        print("No duplicate times in the time series.")


def save_daily_files(dataset_path, output_directory):
    """
    Save each day of the dataset to a separate NetCDF file.

    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
        output_directory (str): Directory to save the daily NetCDF files.
    """
    # Load the dataset
    ds = xr.open_dataset(dataset_path)

    # Loop over each unique day in the 'time' dimension
    for day in ds.time.dt.strftime('%Y%m%d').values:
        # Select the data for the specific day
        ds_day = ds.sel(time=day)

        # Define the output file name based on the date
        output_file = f'{output_directory}/SURF_{day}.nc'

        # Save the daily data to a new NetCDF file
        ds_day.to_netcdf(output_file)
        print(f"Saved {output_file}")



def handle_forecast_file(era5_file_path, prefix="PLEV", archive=True):
    """
    Delete or archive the corresponding forecast file when an ERA5 file is downloaded.

    Parameters:
        era5_file_path (str): The path to the downloaded ERA5 file in the format '{prefix}_YYYYMMDD.nc'.
        prefix (str): The prefix used in the filenames ('PLEV' or 'SURF').
        archive (bool): If True, move the forecast file to the 'archive_forecast' directory. If False, delete the forecast file.
    """
    # Extract the date from the ERA5 filename
    era5_filename = os.path.basename(era5_file_path)
    date_str = era5_filename.split('_')[1].split('.')[0]  # Extract "YYYYMMDD"

    # Only stand down the forecast once its ERA5 replacement is actually on disk.
    # Archiving unconditionally is what turned the failed 2026-08-03 PLEV
    # download into a hard gap: the fallback was moved out of the way for a file
    # that never arrived.
    if not os.path.exists(era5_file_path):
        print(
            f"ERA5 file {era5_file_path} is missing -- keeping the {prefix} forecast "
            f"for {date_str} in place to cover the gap."
        )
        return

    try:
        # Convert the date to the forecast file format "YYYY-MM-DD"
        file_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        # Construct the corresponding forecast file path
        forecast_file = era5_file_path.replace(f"{prefix}_{date_str}.nc", f"{prefix}_FC_{file_date}.nc")

        # Check if the forecast file exists
        if os.path.exists(forecast_file):
            if archive:
                # Create the archive directory if it doesn't exist
                archive_dir = os.path.join(os.path.dirname(forecast_file), 'archive_forecast')
                os.makedirs(archive_dir, exist_ok=True)
                
                # Move the forecast file to the archive directory
                shutil.move(forecast_file, os.path.join(archive_dir, os.path.basename(forecast_file)))
                print(f"Moved forecast file to archive: {os.path.join(archive_dir, os.path.basename(forecast_file))}")
            else:
                # Delete the forecast file
                os.remove(forecast_file)
                print(f"Deleted forecast file: {forecast_file}")
        else:
            print(f"No corresponding forecast file found for {file_date}.")
    
    except ValueError as e:
        print(f"Error processing the filename {era5_filename}: {e}")

# Example usage:
# handle_forecast_file("/path/to/downloaded/PLEV_20240926.nc", prefix="PLEV", archive=True)
# handle_forecast_file("/path/to/downloaded/SURF_20240926.nc", prefix="SURF", archive=False)


# https://forum.ecmwf.int/t/forthcoming-update-to-the-format-of-netcdf-files-produced-by-the-conversion-of-grib-data-on-the-cds/7772

import xarray as xr
import pandas as pd
from pathlib import Path

def merge_climate_files(data_dir, prefix, output_file):
    """
    Merge daily reanalysis, single-day forecasts, and continuous forecast for a given prefix.

    Parameters:
        data_dir (Path or str): Directory containing the NetCDF files.
        prefix (str): File prefix, e.g., "SURF" or "PLEV".
        output_file (Path or str): Path for merged output NetCDF.
    """
    data_dir = Path(data_dir)

    # 1. ERA5 daily reanalysis
    era5_files = sorted(data_dir.glob(f"{prefix}_20*.nc"))
    ds_era5 = xr.concat([xr.open_dataset(f) for f in era5_files], dim="time")

    # 2. Single-day forecasts (fill gaps)
    fc_daily_files = sorted(data_dir.glob(f"{prefix}_FC_20*.nc"))

    era5_times = pd.to_datetime(ds_era5.time.values)
    expected_dates = pd.date_range(start=era5_times.min(), end=era5_times.max(), freq="D")
    missing_dates = [d for d in expected_dates if d not in era5_times]

    ds_fc_daily = None
    if missing_dates:
        ds_fc_daily = xr.concat(
            [xr.open_dataset(f) for f in fc_daily_files if pd.to_datetime(f.name.split("_")[-1].split(".")[0]) in missing_dates],
            dim="time"
        )

    # 3. Continuous 10-day forecast
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = xr.open_dataset(fc_cont_file)

    # 4. Merge in correct order
    datasets = [ds_era5]
    if ds_fc_daily is not None:
        datasets.append(ds_fc_daily)
    datasets.append(ds_fc_cont)

    ds_merged = xr.concat(datasets, dim="time")

    # 5. Save
    ds_merged.to_netcdf(output_file)
    print(f"[{prefix}] Merged file written to {output_file}")
    print(f"[{prefix}] Time coverage: {ds_merged.time.values.min()} to {ds_merged.time.values.max()}")



import re
from pathlib import Path
import pandas as pd
import xarray as xr

def merge_climate_files2(data_dir, prefix, output_file):
    data_dir = Path(data_dir)

    # 1. ERA5 hourly reanalysis
    era5_files = sorted(data_dir.glob(f"{prefix}_20*.nc"))
    ds_era5 = xr.open_mfdataset(era5_files, combine="by_coords")

    # 2. Find gaps in ERA5 coverage
    era5_times = pd.to_datetime(ds_era5.time.values)
    expected_times = pd.date_range(start=era5_times.min(), end=era5_times.max(), freq="H")
    missing_times = set(expected_times) - set(era5_times)

    # 3. Single-day forecasts (only where ERA5 is missing)
    fc_daily_files = sorted(data_dir.glob(f"{prefix}_FC_20*.nc"))
    selected_fc_files = []
    for f in fc_daily_files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
        if m:
            # forecast files should cover full 24h → include if any of its hours are missing
            file_date = pd.to_datetime(m.group(1))
            hours = pd.date_range(file_date, file_date + pd.Timedelta("23H"), freq="H")
            if any(h in missing_times for h in hours):
                selected_fc_files.append(f)

    ds_fc_daily = None
    if selected_fc_files:
        ds_fc_daily = xr.open_mfdataset(selected_fc_files, combine="by_coords")

    # 4. Continuous forecast
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = xr.open_dataset(fc_cont_file)

    # 5. Merge with priority: ERA5 > daily forecast > continuous forecast
    ds_base = ds_era5
    if ds_fc_daily is not None:
        ds_base = ds_base.combine_first(ds_fc_daily)
    ds_merged = ds_base.combine_first(ds_fc_cont)

    # 6. Save
    ds_merged.to_netcdf(output_file)






import re
from pathlib import Path
import pandas as pd
import xarray as xr

def _to_ref_grid(ds, ref_lat, ref_lon):
    """Put ds on the reference grid, skipping the interp when it already matches."""
    if ds.latitude.equals(ref_lat) and ds.longitude.equals(ref_lon):
        return ds
    return ds.interp(latitude=ref_lat, longitude=ref_lon)


def _borrow_hours(hours, day, fc_daily_by_day, ds_fc_cont, ref_lat, ref_lon):
    """Fetch `hours` for `day` from the daily forecast, falling back to the continuous one.

    Both sources are small (24 h and 10 days respectively), so selecting an
    explicit hour list off them is cheap even though it collapses chunking.

    Returns None when neither source covers any of the requested hours.
    """
    if day in fc_daily_by_day:
        ds = _to_ref_grid(
            xr.open_dataset(fc_daily_by_day[day], chunks={"time": 24}), ref_lat, ref_lon
        )
        avail = pd.DatetimeIndex(pd.to_datetime(ds.time.values)).intersection(hours)
        if len(avail):
            return ds.sel(time=avail), "fc_daily"

    if ds_fc_cont is not None:
        avail = pd.DatetimeIndex(pd.to_datetime(ds_fc_cont.time.values)).intersection(hours)
        if len(avail):
            return ds_fc_cont.sel(time=avail), "fc_cont"

    return None, None


def merge_climate_files3(data_dir, prefix, output_file):
    """Assemble the season's hourly forcing from disjoint, time-ordered pieces.

    Source priority is unchanged: ERA5 reanalysis first, then the single-day
    forecast that covers a missing ERA5 day, then the continuous 10-day
    forecast for the tail. What changed is how the pieces are combined.

    The previous version stacked two ``combine_first`` calls. Those outer-join
    onto a union time index, and that reindex collapses dask's time chunking --
    so the supposedly streamed write ended up materialising whole variables
    (PLEV is 6 float32 vars x 8449 steps = 6.5 GB uncompressed). It peaked at
    15.4 GB RSS and was OOM-killed on this 15 GiB instance on 2026-08-09,
    leaving a header-only output whose longitude coordinate was entirely NaN,
    which then broke run_latest with "All-NaN slice encountered".

    Here each day of the season contributes exactly one piece, and the pieces
    are built in chronological order, so assembly is a plain ``xr.concat``
    along an already-sorted dimension -- no alignment, no reindex, chunking
    preserved end to end. Peak memory is one time-chunk.

    Output is left uncompressed, as before: run_latest re-reads this file once
    per point (2000 points), and compression would trade a slower pipeline for
    disk we are not short of.
    """
    data_dir = Path(data_dir)

    # 1. Index the available sources by the day they cover.
    era5_by_day = {}
    for f in sorted(data_dir.glob(f"{prefix}_20*.nc")):
        m = re.fullmatch(rf"{re.escape(prefix)}_(\d{{8}})\.nc", f.name)
        if m:
            era5_by_day[pd.Timestamp(m.group(1))] = f
    if not era5_by_day:
        raise FileNotFoundError(f"No ERA5 {prefix}_YYYYMMDD.nc files found in {data_dir}")

    fc_daily_by_day = {}
    for f in sorted(data_dir.glob(f"{prefix}_FC_20*.nc")):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
        if m:
            fc_daily_by_day[pd.Timestamp(m.group(1))] = f

    first_day, last_day = min(era5_by_day), max(era5_by_day)

    # 2. Reference grid comes from the first ERA5 day; everything else is put on it.
    with xr.open_dataset(era5_by_day[first_day]) as ds0:
        ref_lat = ds0.latitude.load()
        ref_lon = ds0.longitude.load()
        ref_level = ds0.level.load() if "level" in ds0.coords else None

    # 3. Continuous forecast, used for the tail and as last-resort gap fill.
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = None
    if fc_cont_file.exists():
        ds_fc_cont = _to_ref_grid(
            xr.open_dataset(fc_cont_file, chunks={"time": 24}), ref_lat, ref_lon
        )
    else:
        print(f"[{prefix}] WARNING: no continuous forecast at {fc_cont_file}")

    # 4. One piece per day, in order. Disjoint by construction.
    pieces = []
    filled_days, gap_days = [], []

    for day in pd.date_range(first_day, last_day, freq="D"):
        want = pd.date_range(day, day + pd.Timedelta("23h"), freq="h")

        if day in era5_by_day:
            piece = xr.open_dataset(era5_by_day[day], chunks={"time": 24})
        else:
            piece, source = _borrow_hours(
                want, day, fc_daily_by_day, ds_fc_cont, ref_lat, ref_lon
            )
            if piece is None:
                gap_days.append(day.date())
                continue
            filled_days.append((day.date(), source))

        # Patch a short ERA5 day (rare, but it used to be handled hour-by-hour).
        have = pd.DatetimeIndex(pd.to_datetime(piece.time.values))
        short = want.difference(have)
        if len(short):
            patch, source = _borrow_hours(
                short, day, fc_daily_by_day, ds_fc_cont, ref_lat, ref_lon
            )
            if patch is not None:
                # <=24 steps, so sorting inside the day costs nothing.
                piece = xr.concat([piece, patch], dim="time").sortby("time")
                filled_days.append((day.date(), f"{source} (partial: {len(short)}h)"))
            else:
                print(f"[{prefix}] {day.date()}: {len(short)}h missing, no source to fill")

        if ref_level is not None and "level" in piece.coords:
            if not piece.level.equals(ref_level):
                raise ValueError(
                    f"[{prefix}] {day.date()}: pressure levels {piece.level.values} "
                    f"do not match reference {ref_level.values}"
                )
        pieces.append(piece)

    # 5. Tail: continuous forecast beyond the last ERA5 day.
    if ds_fc_cont is not None:
        cont_times = pd.DatetimeIndex(pd.to_datetime(ds_fc_cont.time.values)).sort_values()
        tail = cont_times[cont_times > (last_day + pd.Timedelta("23h"))]
        if len(tail):
            pieces.append(ds_fc_cont.sel(time=tail))
            print(f"[{prefix}] forecast tail: {tail.min()} -> {tail.max()} ({len(tail)}h)")

    if filled_days:
        print(f"[{prefix}] gap-filled {len(filled_days)} day(s): {filled_days}")
    if gap_days:
        print(f"[{prefix}] WARNING: {len(gap_days)} day(s) with no source at all: {gap_days}")

    # 6. Pieces are already sorted and non-overlapping: join="override" keeps the
    # non-concat coords as-is so float noise in lat/lon cannot trigger an
    # outer join (the failure mode that made this blow up in the first place).
    ds_merged = xr.concat(
        pieces, dim="time", join="override", combine_attrs="override"
    )

    # 7. Streamed write: dask computes one time-chunk at a time, single-threaded
    # to keep peak RAM predictable and avoid worker contention on small instances.
    delayed = ds_merged.to_netcdf(output_file, compute=False)
    delayed.compute(scheduler="single-threaded")


def validate_merged_file(output_file, prefix):
    """Refuse to hand a corrupt merged file to the downscaling step.

    The 2026-08-09 failure wrote a file that opened cleanly but held nothing:
    every longitude NaN, every value NaN. run_latest only discovered that
    2000 deleted point files later, deep inside a multiprocessing pool. These
    checks are cheap and turn that into an immediate, legible failure.
    """
    with xr.open_dataset(output_file) as ds:
        for coord in ("latitude", "longitude", "time"):
            if coord not in ds.coords:
                raise RuntimeError(f"[{prefix}] merged file has no '{coord}' coordinate")

        for coord in ("latitude", "longitude"):
            if bool(ds[coord].isnull().any()):
                n = int(ds[coord].isnull().sum())
                raise RuntimeError(
                    f"[{prefix}] {n}/{ds[coord].size} {coord} values are NaN -- "
                    f"{output_file} is a header-only shell, most likely a killed write"
                )

        times = pd.DatetimeIndex(pd.to_datetime(ds.time.values))
        if times.size == 0:
            raise RuntimeError(f"[{prefix}] merged file has no timesteps")
        if times.has_duplicates:
            dupes = times[times.duplicated()].unique()
            raise RuntimeError(f"[{prefix}] duplicate timestamps: {list(dupes[:5])}")
        if not times.is_monotonic_increasing:
            raise RuntimeError(f"[{prefix}] timestamps are not monotonically increasing")

        # A killed write leaves the data all-NaN even when the header looks sane.
        for name in list(ds.data_vars)[:1]:
            for label, idx in (("first", 0), ("last", -1)):
                if bool(ds[name].isel(time=idx).isnull().all()):
                    raise RuntimeError(
                        f"[{prefix}] {name} is entirely NaN at the {label} timestep "
                        f"({times[idx]}) -- merged file is not usable"
                    )

        gaps = pd.date_range(times.min(), times.max(), freq="h").difference(times)
        if len(gaps):
            print(f"[{prefix}] WARNING: {len(gaps)} missing hour(s), e.g. {list(gaps[:5])}")

        print(
            f"[{prefix}] validated {output_file}: {times.size} steps, "
            f"{times.min()} -> {times.max()}"
        )



def main():
    start_time = datetime.now()
    mydir = sys.argv[1]
    os.chdir(mydir)

    config_file = './config.yml'
    
    # Initialize Topoclass and perform operations
    mp = tc.Topoclass(config_file)

    # download latest climate data
    date_str = mp.get_lastday()
    #date_str = "2024-09-25" 
    lastday = datetime.strptime(date_str, '%Y-%m-%d')
    print ("Downloading from CDS PLEV and SURF for day: " +date_str)




    # # Create a ThreadPoolExecutor to run functions concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit both functions to run in parallel
        future_surf = executor.submit(mp.get_era5_snowmapper, 'surf', lastday)
        future_plev = executor.submit(mp.get_era5_snowmapper, 'plev', lastday)

        # .result() re-raises in this thread. concurrent.futures.wait() does not,
        # which is how the 2026-08-03 PLEV download failed in total silence: SURF
        # landed, PLEV did not, and the run carried on and archived away the
        # forecast file that would have covered the gap.
        errors = []
        for name, future in (("surf", future_surf), ("plev", future_plev)):
            try:
                future.result()
            except Exception as exc:
                errors.append(f"{name}: {type(exc).__name__}: {exc}")
        if errors:
            raise RuntimeError(
                "ERA5 download failed for %s: %s" % (date_str, "; ".join(errors))
            )

        # Continue the rest of your script after both functions are done
        print("Both functions finished, continuing with the rest of the script.")
        downloadedPLEVfile = target = mp.config.climate.path  + "/forecast/PLEV_%04d%02d%02d.nc" % (lastday.year, lastday.month, lastday.day)
        downloadedSURFfile = target = mp.config.climate.path  + "/forecast/SURF_%04d%02d%02d.nc" % (lastday.year, lastday.month, lastday.day)

    mp.process_SURF_file('./inputs/climate/forecast')
    mp.remap_netcdf('./inputs/climate/forecast'  )



    # check for existing forecast file and delete if era5 exists
    handle_forecast_file(downloadedPLEVfile, prefix="PLEV", archive=True)
    handle_forecast_file(downloadedSURFfile, prefix="SURF", archive=True)

    data_dir = "./inputs/climate/forecast"
    out_dir = "./inputs/climate"
    # List of files to remove if they exist
    files_to_remove = [
        os.path.join(out_dir, "SURF_final_merged_output.nc"),
        os.path.join(out_dir, "PLEV_final_merged_output.nc")
    ]

    # Remove each file if it exists
    for f in files_to_remove:
        if os.path.exists(f):
             os.remove(f)

    
    # Merge, then prove the result is usable before run_latest ever sees it.
    for prefix in ("SURF", "PLEV"):
        merged = os.path.join(out_dir, f"{prefix}_final_merged_output.nc")
        merge_climate_files3(data_dir, prefix, merged)
        try:
            validate_merged_file(merged, prefix)
        except Exception:
            # Leave nothing behind that a later step could mistake for good data.
            if os.path.exists(merged):
                os.remove(merged)
                print(f"[{prefix}] removed unusable {merged}")
            raise






    print(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    main()


