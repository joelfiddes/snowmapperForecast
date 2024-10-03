import os
import re
import glob
import sys
from TopoPyScale import sim_fsm as sim

def create_directory(path):
    """
    Create a directory if it doesn't already exist.
    
    Parameters:
    - path (str): Path to the directory.
    """
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"The new directory '{path}' is created!")

def natural_sort(l):
    """
    Sort a list in a human-readable order (natural sorting).
    
    Parameters:
    - l (list): List of strings to sort.
    
    Returns:
    - list: Naturally sorted list.
    """
    def convert(text): 
        return int(text) if text.isdigit() else text.lower()

    def alphanum_key(key): 
        return [convert(c) for c in re.split('([0-9]+)', key)]
    
    return sorted(l, key=alphanum_key)

def get_file_names(file_paths):
    """
    Extract file names from file paths.
    
    Parameters:
    - file_paths (list): List of file paths.
    
    Returns:
    - list: List of file names.
    """
    return [os.path.basename(file_path) for file_path in file_paths]

def concat_fsm(mydir):
    """
    Concatenate FSM results from separate years into a single file.
    
    Parameters:
    - mydir (str): Directory containing the simulation outputs.
    """
    output_dir = os.path.join(mydir, "outputs")
    create_directory(output_dir)

    sim_folders = glob.glob(os.path.join(mydir, "sim_*/"))
    file_paths = glob.glob(os.path.join(sim_folders[0], "outputs/FSM_pt*"))
    
    filenames = natural_sort(get_file_names(file_paths))

    for myname in filenames:
        matching_files = natural_sort(glob.glob(os.path.join(mydir, "sim_*/outputs/", myname)))
        
        with open(os.path.join(output_dir, myname), 'w') as outfile:
            for fname in matching_files:
                with open(fname) as infile:
                    outfile.write(infile.read())


import os
import glob
import pandas as pd



import os
import glob
import pandas as pd

def concat_fsm_with_overwrite(mydir):
    """
    Concatenate FSM results from 'sim_archive' and 'sim_latest' directories,
    prioritizing the data from 'sim_latest' in case of overlapping timestamps.
    
    Parameters:
    - mydir (str): Directory containing 'sim_archive' and 'sim_latest' subdirectories.
    """
    archive_dir = os.path.join(mydir, "sim_archive/outputs")
    latest_dir = os.path.join(mydir, "sim_latest/outputs")
    output_dir = os.path.join(mydir, "outputs")
    
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

    # Get FSM txt output files from both directories (assuming the same file structure)
    archive_files = glob.glob(os.path.join(archive_dir, "FSM_pt*.txt"))
    latest_files = glob.glob(os.path.join(latest_dir, "FSM_pt*.txt"))

    # Extract file names and ensure no duplicates
    filenames = set(os.path.basename(path) for path in archive_files + latest_files)

    # Loop over each file name
    for myname in filenames:
        archive_file = os.path.join(archive_dir, myname)
        latest_file = os.path.join(latest_dir, myname)

        # Initialize an empty DataFrame to hold combined data
        combined_df = pd.DataFrame()

        # If archive file exists, load it into a DataFrame (space-delimited, no header)
        if os.path.exists(archive_file):
            archive_df = pd.read_csv(archive_file, delim_whitespace=True, header=None)
            # Create a temporary datetime column for merging, but don't keep it in the final output
            archive_df['time'] = pd.to_datetime(archive_df.iloc[:, [0, 1, 2, 3]].rename(columns={0: 'year', 1: 'month', 2: 'day', 3: 'hour'}))
            combined_df = archive_df
        
        # If latest file exists, load it and overwrite overlapping data
        if os.path.exists(latest_file):
            latest_df = pd.read_csv(latest_file, delim_whitespace=True, header=None)
            # Create a temporary datetime column for merging, but don't keep it in the final output
            latest_df['time'] = pd.to_datetime(latest_df.iloc[:, [0, 1, 2, 3]].rename(columns={0: 'year', 1: 'month', 2: 'day', 3: 'hour'}))
            
            if not combined_df.empty:
                # Combine the two, prioritizing the data from latest_df
                combined_df = pd.concat([combined_df, latest_df]).drop_duplicates(subset='time', keep='last')
            else:
                combined_df = latest_df
        
        # Drop the temporary time column before saving
        combined_df = combined_df.drop(columns=['time'])

        # Save the combined DataFrame to the output directory without header, and preserve the original format
        if not combined_df.empty:
            output_file = os.path.join(output_dir, myname)
            combined_df.to_csv(output_file, index=False, header=False, sep=' ')
            print(f"Saved {output_file}")



# Example usage
mydir = './D100'
concat_fsm_with_overwrite(mydir)





def sort_data_hydro(directory):
    """
    Sort data in hydrological year format within text files.
    
    Parameters:
    - directory (str): Directory containing the text files to sort.
    """
    def get_month_day(line):
        parts = line.split()
        return int(parts[1]), int(parts[2])

    def to_hydrological_year(month, day):
        return (month + 12, day) if month < 9 else (month, day)

    def sort_by_hydrological_year(data):
        return sorted(data, key=lambda x: to_hydrological_year(*get_month_day(x)))

    def process_file(filename):
        with open(filename, 'r') as file:
            data = file.readlines()
        return sort_by_hydrological_year(data)

    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            sorted_data = process_file(os.path.join(directory, filename))
            with open(os.path.join(directory, filename), 'w') as file:
                file.writelines(sorted_data)

def simulate_fsm(mydir):
    """
    Simulate FSM using the concatenated files.
    
    Parameters:
    - mydir (str): Directory containing the simulation outputs.
    """
    os.chdir(mydir)
    fsm_files = glob.glob("./outputs/FSM*")

    for myfile in fsm_files:
        nsim = re.search(r'(?<=_)\d+', myfile).group()
        sim.fsm_nlst(31, myfile, 24)
        sim.fsm_sim(f"./fsm_sims/nlst_FSM_pt_{nsim}.txt", "./FSM")

def main(mydir):
    concat_fsm(mydir)
    #sort_data_hydro(os.path.join(mydir, "outputs"))
    simulate_fsm(mydir)
    print("FSM simulation complete.")

if __name__ == "__main__":
    mydir = sys.argv[1]
    main(mydir)
    
