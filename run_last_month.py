import os
import sys
import shutil
import fnmatch
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from netCDF4 import Dataset, num2date
from TopoPyScale import topoclass as tc

def load_config(config_file):
    """
    Load configuration using Topoclass.
    
    Parameters:
    - config_file (str): Path to the configuration file.
    
    Returns:
    - mp (Topoclass): Loaded Topoclass object with configuration.
    """
    mp = tc.Topoclass(config_file)
    return mp

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

def update_config_paths(mp, newdir, thisyear, thismonth, daysinmonth):
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
    print(newdir)
    mp.config.outputs.downscaled = Path(os.path.join(newdir, 'outputs', 'downscaled/'))
    mp.config.outputs.path = Path(os.path.join(newdir, 'outputs/'))
    mp.config.outputs.tmp_path = os.path.join(mp.config.outputs.path, 'tmp/')

    mp.config.project.start = mp.config.project.start.replace(year=thisyear, month=thismonth, day=1)
    mp.config.project.end = mp.config.project.end.replace(year=thisyear, month=thismonth, day=daysinmonth)

def perform_simulation(mp):
    """
    Perform the simulation steps using the updated configuration.
    
    Parameters:
    - mp (Topoclass): The Topoclass object with the loaded configuration.
    """
    if os.path.exists("./outputs/ds_solar.nc"):
        os.remove("./outputs/ds_solar.nc")

    mp.extract_topo_param() # this loads existing datasets
    mp.compute_horizon()
    mp.compute_solar_geometry()
    mp.downscale_climate()
    mp.to_fsm()

def main(mydir):
    os.chdir(mydir)
    start_time = datetime.now()

    # Load configuration
    config_file = './config.yml'
    mp = load_config(config_file)
    mainwdir = mp.config.project.directory
    start_year = mp.config.project.start.year

    # Determine current month and year
    thismonth = start_time.month - 1  # Adjust for previous month
    thisyear = start_year if thismonth in {9, 10, 11, 12} else start_year + 1

    # Get the last timestamp and determine the number of days in the month
    nc_file = f'../master/inputs/climate/SURF_{thisyear:04d}{thismonth:02d}.nc'
    last_timestamp = get_last_timestamp(nc_file)
    daysinmonth = determine_days_in_month(last_timestamp)

    print(f"thismonth = {thismonth}")
    print(f"thisyear = {thisyear}")
    print(f"daysinmonth = {daysinmonth}")

    # Prepare the output directory
    newdir = os.path.join(mainwdir, f"sim_{thismonth}/")
    clean_and_prepare_output_dir(mainwdir, newdir)

    # Update configuration paths
    update_config_paths(mp, newdir, thisyear, thismonth, daysinmonth)

    # Perform the simulation
    perform_simulation(mp)

    print(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    mydir = sys.argv[1]
    main(mydir)
