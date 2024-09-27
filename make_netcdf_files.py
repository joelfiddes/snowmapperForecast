import os
import sys
from munch import DefaultMunch
from TopoPyScale import sim_fsm as sim

def load_config(config_file):
    """
    Load the configuration file using DefaultMunch.
    
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
        print(f'ERROR: config file does not exist. \n\t Current file path: {config_file}\n\t Current working directory: {os.getcwd()}')
        sys.exit(1)

def process_variable(var_name, unit, epsg, dem_res):
    """
    Process a specific variable and write it to a NetCDF file.
    
    Parameters:
    - var_name (str): Name of the variable to process (e.g., "swe", "snd").
    - unit (str): Unit of the variable (e.g., "mm", "m").
    - config (DefaultMunch): Loaded configuration object.
    - dem_res (float): DEM resolution from the configuration.
    """
    if var_name == "swe":
        output_var_name = "SWE"
    if var_name == "snd":
        output_var_name = "HS"
    if var_name == "rof":
        output_var_name = "ROF"

    df = sim.agg_by_var_fsm(var=var_name)
    grid_stack, lats, lons = sim.topo_map_sim(df, 1, "float32", dem_res)
    sim.write_ncdf(".", grid_stack, var_name, unit, epsg, dem_res, df.index.array, lats, lons, "float32", True, output_var_name)

def main(mydir):
    """
    Main function to process snow variables (SWE and SND) and save them as NetCDF files.
    
    Parameters:
    - mydir (str): Directory containing the project.
    """
    os.chdir(mydir)
    config_file = './config.yml'
    
    config = load_config(config_file)
    
    # Process and write snow water equivalent (SWE)
    process_variable("swe", "mm", config.dem.epsg, config.dem.dem_resol)
    
    # Process and write snow depth (SND)
    process_variable("snd", "m", config.dem.epsg, config.dem.dem_resol)

    # Process and write snow runoff (ROF)
    process_variable("rof", "mm", config.dem.epsg, config.dem.dem_resol)

if __name__ == "__main__":
    mydir = sys.argv[1]
    main(mydir)
