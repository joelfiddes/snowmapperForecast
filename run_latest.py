import os
import sys
import shutil
import glob
import fnmatch
from pathlib import Path
from datetime import datetime, timedelta
from netCDF4 import Dataset, num2date
from TopoPyScale import topoclass as tc



def get_last_timestamp(nc_file):
    """
    Extract the last timestamp from a NetCDF file.
    
    Parameters:
    - nc_file (str): Path to the NetCDF file.
    
    Returns:
    - last_timestamp (datetime): The last timestamp in the NetCDF file.
    """
    with Dataset(nc_file, 'r') as nc_dataset:
        time_variable = nc_dataset.variables['time']
        time_values = time_variable[:]
        timestamps = num2date(time_values, units=time_variable.units, calendar=time_variable.calendar)
        last_timestamp = timestamps[-1]
    return last_timestamp

def get_last_fullday_timestamp(nc_file):
    import xarray as xr
    import pandas as pd

    # Load the dataset
    ds = xr.open_dataset(nc_file)

    # Convert the time variable to a pandas DatetimeIndex
    time_series = pd.to_datetime(ds['time'].values)

    # Filter the time series to get only the timestamps where hour == 23
    filtered_times = time_series[time_series.hour == 23]

    # Get the last timestamp where hour == 23
    if len(filtered_times) > 0:
        last_timestamp = filtered_times[-1]
        print(f"The last timestamp where hour is 23 (fullday): {last_timestamp}")
    else:
        print("No timestamp found where hour is 23.")
    return last_timestamp


def determine_days_in_month(last_timestamp):
    """
    Determine the number of days in the current month based on the last timestamp.
    
    Parameters:
    - last_timestamp (datetime): The last timestamp in the NetCDF file.
    
    Returns:
    - daysinmonth (int): The number of days in the month.
    """
    return last_timestamp.day if last_timestamp.hour == 23 else last_timestamp.day - 1

def clean_and_prepare_output_dir(mainwdir, newdir):
    """
    Clean the main output directory and copy its contents to a new directory,
    excluding files matching the pattern 'FSM_pt_*.txt'.
    
    Parameters:
    - mainwdir (str): Main working directory.
    - newdir (str): New directory for the simulation outputs.
    """
    source_dir = os.path.join(mainwdir, "outputs")
    destination_dir = os.path.join(newdir, "outputs")
    
    # Function to ignore files matching the pattern 'FSM_pt_*.txt'
    # Function to ignore files matching the patterns 'FSM_pt_*.txt', '*HS.nc', and '*SWE.nc'
    def ignore_files(dir, files):
        ignore_patterns = ['FSM_pt_*.txt', '*HS.nc', '*SWE.nc']
        return [f for f in files if any(fnmatch.fnmatch(f, pattern) for pattern in ignore_patterns)]
    
    # Remove the new directory if it exists
    if os.path.exists(newdir):
        shutil.rmtree(newdir)
    
    # Copy the output directory to the new location, ignoring 'FSM_pt_*.txt' files
    if os.path.exists(source_dir):
        shutil.copytree(source_dir, destination_dir, ignore=ignore_files)
    else:
        raise FileNotFoundError(f"Source directory '{source_dir}' does not exist.")
    
    # Copy the FSM file if it exists (but do not exclude any specific FSM files)
    src = os.path.join(mainwdir, "FSM")
    dst = os.path.join(newdir, "FSM")
    if os.path.exists(src):
        shutil.copyfile(src, dst)
        shutil.copymode(src, dst)  # Copy the file mode
    else:
        raise FileNotFoundError(f"FSM file '{src}' does not exist.")

def update_config_paths(mp, newdir, startDate, endDate):
    """
    Update the paths and parameters in the configuration file.
    
    Parameters:
    - mp (Topoclass): The Topoclass object with the loaded configuration.
    - newdir (str): The new directory for simulation outputs.
    - thisyear (int): The current year.
    - thismonth (int): The current month.
    - daysinmonth (int): The number of days in the month.
    """
    mp.config.project.directory = newdir
    mp.config.outputs.downscaled = Path(os.path.join(newdir, 'outputs', 'downscaled'))
    mp.config.outputs.path = Path(os.path.join(newdir, 'outputs/'))
    mp.config.outputs.tmp_path = os.path.join(mp.config.outputs.path, 'tmp/')

    mp.config.project.start = mp.config.project.start.replace(year=startDate.year, month=startDate.month, day=startDate.day)
    mp.config.project.end = mp.config.project.end.replace(year=endDate.year, month=endDate.month, day=endDate.day)
    # mp.config.project.end = mp.config.project.end.replace(year=thisyear, month=thismonth, day=1)

def perform_simulation(mp):
    """
    Perform the simulation steps using the updated configuration.
    
    Parameters:
    - mp (Topoclass): The Topoclass object with the loaded configuration.
    """
    if os.path.exists(mp.config.project.directory+"/outputs/ds_solar.nc"):
        os.remove(mp.config.project.directory+"/outputs/ds_solar.nc")
    mp.extract_topo_param()
    mp.compute_horizon()
    mp.compute_solar_geometry()
    mp.downscale_climate()
    mp.to_fsm()

def clamp_start_past_gaps(first_timestamp, last_timestamp,
                          plev_file='../master/inputs/climate/PLEV_final_merged_output.nc'):
    """Move the window start forward past any hole in the merged climate.

    downscale_climate() does a strict `.sel(time=date_range(start, end))` on the
    PLEV file (topo_scale.py:448), so a SINGLE missing hour inside the window
    kills the whole run with
        KeyError: "not all values found in index 'time'"

    That happened on 2026-09-30: the cron was disabled 09-28 11:50 -> 09-30 08:50
    for the season rollover, so no IFS forecast was captured for the 09-28 or
    09-29 initialisations, and ERA5 (~6 days behind) had not reached them. The
    merged files carried a real 48-hour hole, every nightly run failed, the sim
    never extended, and the downstream calculator raised "expected 10 days
    forecast, got 7". Left alone it would have kept failing until the rolling
    7-day window moved past the hole, about a week later.

    A hole is a genuine data outage, not something to interpolate over, so we
    shorten the refresh window rather than invent values -- and say so loudly.
    Anything before the hole simply keeps the values it already has.
    """
    import pandas as pd
    import xarray as xr

    try:
        with xr.open_dataset(plev_file) as ds:
            have = pd.DatetimeIndex(pd.to_datetime(ds.time.values))
    except Exception as exc:
        print(f"WARNING: could not inspect {plev_file} for gaps ({exc}); "
              f"leaving the window unchanged")
        return first_timestamp

    want = pd.date_range(pd.Timestamp(first_timestamp).floor('H'),
                         pd.Timestamp(last_timestamp), freq='H')
    missing = want.difference(have)
    if not len(missing):
        return first_timestamp

    new_start = (missing.max() + pd.Timedelta(hours=1)).to_pydatetime()
    print(f"WARNING: {len(missing)} hour(s) missing from the merged climate "
          f"between {missing.min()} and {missing.max()}")
    print(f"         shortening the refresh window: {first_timestamp} -> {new_start}")
    if new_start >= pd.Timestamp(last_timestamp).to_pydatetime():
        raise SystemExit(
            "ABORT: the data gap reaches the end of the window "
            f"({missing.max()} >= {last_timestamp}); nothing left to downscale. "
            "Wait for ERA5 to backfill, or re-fetch the missing forecast "
            "initialisations with IFS_FETCH_DATE.")
    return new_start


def main(mydir):
    os.chdir(mydir)
    start_time = datetime.now()
    first_timestamp = start_time - timedelta(days=7)

    # Load configuration
    config_file = './config.yml'
    mp = tc.Topoclass(config_file)
    mainwdir = mp.config.project.directory


    # Get the last timestamp of last forecast day
    nc_file = f'../master/inputs/climate/SURF_final_merged_output.nc'
    last_timestamp = get_last_fullday_timestamp(nc_file)

    first_timestamp = clamp_start_past_gaps(first_timestamp, last_timestamp)

    print(f"First timestamp: {first_timestamp}")
    print(f"Last fullday timestamp: {last_timestamp}")
    # strftime('%Y%m%d')




    #startDate= "%04d-%02d-%02d" % (start_time_7daysago.year, start_time_7daysago.month, start_time_7daysago.day)
    #endDate = "%04d-%02d-%02d" % (last_timestamp.year, last_timestamp.month, last_timestamp.day)


    # Prepare the output directory
    newdir = os.path.join(mainwdir, f"sim_latest/")
    clean_and_prepare_output_dir(mainwdir, newdir)

    # Update configuration paths
    update_config_paths(mp, newdir, first_timestamp, last_timestamp)

    # Perform the simulation
    perform_simulation(mp)

    print(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    mydir = sys.argv[1]
    main(mydir)








