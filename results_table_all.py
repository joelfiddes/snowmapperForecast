import os
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np
import pandas as pd
import sys
import glob
import re
from datetime import datetime

startTime = datetime.now()
thismonth = startTime.month    # now
thisyear = startTime.year 
year = str([thisyear if thismonth in {9, 10, 11, 12} else thisyear-1][0])
print(year)



# Load the shapefile containing polygons
shapefile_path = "./master/inputs/basins/basins.shp"
polygons = gpd.read_file(shapefile_path)
# Directory containing merged reprojected files swe
directory = "./spatial/"

# create output dir for tables
os.makedirs("./tables", exist_ok=True)

# Function to extract mean value within each polygon
def extract_mean_values(merged_raster, polygons):
    mean_values = []
    dates = []
    for idx, geom in enumerate(polygons.geometry):
        # Mask the raster within the polygon
        masked, _ = mask(merged_raster, [geom], crop=True)
        # Calculate mean value
        mean_value = np.nanmean(masked)
        mean_values.append(mean_value)
        # Get date from filename or any other source
        # Assuming you have the date in a variable named 'date'
        date = "YYYY-MM-DD"  # Replace this with the actual date
        dates.append(date)
    return mean_values, dates

def natural_sort(l):
    def convert(text): return int(text) if text.isdigit() else text.lower()
    def alphanum_key(key): return [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)
   

#===============================================================================
# Basin SWE
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
a = glob.glob(directory + "swe_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        
# Extract the 'Date' column
date_column = results_df['Date']

# Save the results to a single CSV file

df_no_date = results_df.drop(columns=['Date'])
# Group columns by their names and calculate the average
averages = df_no_date.groupby(df_no_date.columns, axis=1).mean()

# Concatenate the 'Date' column with the resulting DataFrame
out = pd.concat([date_column, averages], axis=1)

out.to_csv("./tables/swe_basin_mean_values_table.csv", index=False)

#===============================================================================
# Basin HS
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
a = glob.glob(directory + "hs_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        
# Extract the 'Date' column
date_column = results_df['Date']

# Save the results to a single CSV file

df_no_date = results_df.drop(columns=['Date'])
# Group columns by their names and calculate the average
averages = df_no_date.groupby(df_no_date.columns, axis=1).mean()

# Concatenate the 'Date' column with the resulting DataFrame
out = pd.concat([date_column, averages], axis=1)

out.to_csv("./tables/hs_basin_mean_values_table.csv", index=False)



#===============================================================================
# Basin ROF
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
a = glob.glob(directory + "ROF_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        
# Extract the 'Date' column
date_column = results_df['Date']

# Save the results to a single CSV file

df_no_date = results_df.drop(columns=['Date'])
# Group columns by their names and calculate the average
averages = df_no_date.groupby(df_no_date.columns, axis=1).mean()

# Concatenate the 'Date' column with the resulting DataFrame
out = pd.concat([date_column, averages], axis=1)

out.to_csv("./tables/rof_basin_mean_values_table.csv", index=False)






#===============================================================================
# Catchment SWE
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
a = glob.glob(directory + "swe_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        #print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        


results_df.to_csv("./tables/swe_mean_values_table.csv", index=False)

#===============================================================================
# Catchment HS
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
a = glob.glob(directory + "hs_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        #print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        

results_df.to_csv("./tables/hs_mean_values_table.csv", index=False)


#===============================================================================
# Catchment ROF
#===============================================================================

# Create an empty DataFrame to store all results
results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
a = glob.glob(directory + "ROF_merged_reprojected_"+ str(year) + "*")
file_list = natural_sort(a)
    
    
# Iterate over files in the directory
for filename in file_list:
    
        #print(filename)
        
        # Extract date from filename
        date1 = filename.split("_")[3]
        day= filename.split("_")[4].split(".")[0]
        
        date = date1 +"_"+ day
        
        from datetime import datetime, timedelta

        def convert_to_timestamp(date_str):
            # Split the string into year and DOY components
            year, doy = map(int, date_str.split('_'))

            # Calculate the date by adding the DOY to September 1st of that year
            base_date = datetime(year, 9, 1)
            target_date = base_date + timedelta(days=doy)  # Subtract 1 since DOY starts from 1

            return target_date

        # Example usage
        timestamp = convert_to_timestamp(date)




        # Load the merged raster
        merged_raster_path = os.path.join( filename)
        merged_raster = rasterio.open(merged_raster_path)

        # Extract mean values and dates
        mean_values, _ = extract_mean_values(merged_raster, polygons)

        # Append results to the DataFrame
        # results_df[date] = pd.Series(mean_values)
        
        # Append results to the DataFrame
        results_df.loc[len(results_df)] = [timestamp] + mean_values
        

results_df.to_csv("./tables/rof_mean_values_table.csv", index=False)
