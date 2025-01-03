import pandas as pd
import numpy as np
from datetime import datetime, timedelta




# ==============================================================================
# CATCHMENT STATISTICS
# ==============================================================================

# Read the swedata
data = pd.read_csv("./tables/swe_mean_values_table.csv")
        
# Assuming the first column is 'Date' and the rest are catchment codes
catchments = data.columns[1:]


# Iterate over each catchment
for catchment in catchments:

        # Read the swedata
        data = pd.read_csv("./tables/swe_mean_values_table.csv")
        
        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]

        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])

        # small hack to deal with the 3 columns we dont need
        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
                # Merge the quantiles on 'DayOfYear'
        quantiles_merged = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged = pd.merge(quantiles_merged, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_SWE', catchment+'_Q5': 'Q5_SWE', catchment: 'Q95_SWE'}, inplace=True)

        modified_quantiles = quantiles_merged

        # Create a date range from start_date to end_date
        # date_range = pd.date_range(start=start_date, end=end_date)

        # Remove the 'Date' column
        # modified_quantiles = modified_quantiles.drop(columns=['date'])

        # Insert the 'Date' column at position 0
        # modified_quantiles.insert(0, 'date', date_range)


        # Read the hs data
        data = pd.read_csv("./tables/hs_mean_values_table.csv")
        
        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]
        
        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])


        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
        # Merge the quantiles on 'DayOfYear'
        quantiles_merged_hs = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged_hs = pd.merge(quantiles_merged_hs, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged_hs.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_HS', catchment+'_Q5': 'Q5_HS', catchment: 'Q95_HS'}, inplace=True)

        
        # deal with strange spike on 1 Jan by interpolating 31Dec and 2 Jan
        # where does it come from?
        # Concatenate the columns to the modified_quantiles DataFrame
        # modified_quantiles = pd.concat([modified_quantiles, quantiles_merged_hs], axis=1)



        modified_quantiles = pd.merge(modified_quantiles, quantiles_merged_hs, on='date')
        
        

        # ==============================================================================
        # ROF STATISTICS (New section for runoff)
        # ==============================================================================


        # Read the rof data
        data = pd.read_csv("./tables/rof_mean_values_table.csv")

        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]
        
        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])

        # small hack to deal with the 3 columns we don't need
        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
        # Merge the quantiles on 'DayOfYear'
        quantiles_merged_rof = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged_rof = pd.merge(quantiles_merged_rof, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged_rof.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_ROF', catchment+'_Q5': 'Q5_ROF', catchment: 'Q95_ROF'}, inplace=True)

        # Merge with modified_quantiles (existing SWE and HS data)
        modified_quantiles = pd.merge(modified_quantiles, quantiles_merged_rof, on='date')

        # Reorder the columns to include ROF columns
        column_order = ['date', 'Q5_SWE', 'Q5_HS', 'Q5_ROF', 'Q50_SWE', 'Q50_HS', 'Q50_ROF', 'Q95_SWE', 'Q95_HS', 'Q95_ROF']
        modified_quantiles = modified_quantiles.reindex(columns=column_order)

        # # Reset the index to dates
        modified_quantiles.reset_index(inplace=True)

        # Convert the 'Date' column to datetime format
        modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # Set the 'Date' column as the index
        modified_quantiles.set_index('date', inplace=True, drop=False)

        # Write the final output
        filename = f"./tables/{catchment}_current.txt"
        modified_quantiles = modified_quantiles.drop(columns=['index'])


        # Adding FC tag
        # Convert the 'date' column to datetime format if not already
        modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # Get today's date
        today = datetime.today().date()
        
        # Define the start of forecast as end of era5 
        # start_time = datetime.now()
        # today = start_time - timedelta(days=6)

        # Add the 'FC' column with True for dates on or after today, and False for dates before today
        modified_quantiles['FC'] = modified_quantiles['date'].apply(lambda x: x.date() >= today)

        modified_quantiles.set_index('date', inplace=True, drop=False)


        # Reset the index so 'date' becomes a column again
        modified_quantiles = modified_quantiles.reset_index(drop=True)


        modified_quantiles.to_csv(filename, sep='\t', index=False)


# ==============================================================================
# BASIN STATISTICS
# ==============================================================================

# Read the swedata
data = pd.read_csv("./tables/swe_basin_mean_values_table.csv")
        
# Assuming the first column is 'Date' and the rest are catchment codes
catchments = data.columns[1:]


# Iterate over each catchment
for catchment in catchments:

        # Read the swedata
        data = pd.read_csv("./tables/swe_basin_mean_values_table.csv")
        
        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]

        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])

        # small hack to deal with the 3 columns we dont need
        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
                # Merge the quantiles on 'DayOfYear'
        quantiles_merged = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged = pd.merge(quantiles_merged, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_SWE', catchment+'_Q5': 'Q5_SWE', catchment: 'Q95_SWE'}, inplace=True)

        modified_quantiles = quantiles_merged

        # Create a date range from start_date to end_date
        # date_range = pd.date_range(start=start_date, end=end_date)

        # Remove the 'Date' column
        # modified_quantiles = modified_quantiles.drop(columns=['date'])

        # Insert the 'Date' column at position 0
        # modified_quantiles.insert(0, 'date', date_range)


        # Read the swedata
        data = pd.read_csv("./tables/hs_basin_mean_values_table.csv")
        
        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]
        
        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])


        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
        # Merge the quantiles on 'DayOfYear'
        quantiles_merged_hs = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged_hs = pd.merge(quantiles_merged_hs, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged_hs.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_HS', catchment+'_Q5': 'Q5_HS', catchment: 'Q95_HS'}, inplace=True)


        # deal with strange spike on 1 Jan by interpolating 31Dec and 2 Jan
        # where does it come from?
        # Concatenate the columns to the modified_quantiles DataFrame
        # modified_quantiles = pd.concat([modified_quantiles, quantiles_merged_hs], axis=1)
        modified_quantiles = pd.merge(modified_quantiles, quantiles_merged_hs, on='date')


        # Read the rof data
        data = pd.read_csv("./tables/rof_basin_mean_values_table.csv")

        # Assuming the first column is 'Date' and the rest are catchment codes
        catchments = data.columns[1:]
        
        # Select only the desired catchment column
        catchment_data = data[['Date', catchment]].copy()

        # Convert 'Date' column to datetime format
        catchment_data['Date'] = pd.to_datetime(catchment_data['Date'])

        # small hack to deal with the 3 columns we don't need
        Q50 = catchment_data
        Q5 = catchment_data
        Q95 = catchment_data
        
        # Merge the quantiles on 'DayOfYear'
        quantiles_merged_rof = pd.merge(Q50, Q5, on='Date', suffixes=('_Q50', '_Q5'))
        quantiles_merged_rof = pd.merge(quantiles_merged_rof, Q95, on='Date', suffixes=('_Q50', '_Q95'))

        # Rename the columns
        quantiles_merged_rof.rename(columns={'Date': 'date', catchment + '_Q50': 'Q50_ROF', catchment+'_Q5': 'Q5_ROF', catchment: 'Q95_ROF'}, inplace=True)

        # Merge with modified_quantiles (existing SWE and HS data)
        modified_quantiles = pd.merge(modified_quantiles, quantiles_merged_rof, on='date')

        # Reorder the columns to include ROF columns
        column_order = ['date', 'Q5_SWE', 'Q5_HS', 'Q5_ROF', 'Q50_SWE', 'Q50_HS', 'Q50_ROF', 'Q95_SWE', 'Q95_HS', 'Q95_ROF']
        modified_quantiles = modified_quantiles.reindex(columns=column_order)

        # # Reset the index to dates
        modified_quantiles.reset_index(inplace=True)

        # Convert the 'Date' column to datetime format
        modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # Set the 'Date' column as the index
        modified_quantiles.set_index('date', inplace=True, drop=False)

        # Write the final output
        filename = f"./tables/{catchment}_current.txt"
        modified_quantiles = modified_quantiles.drop(columns=['index'])


        # Adding FC tag
        # Convert the 'date' column to datetime format if not already
        modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # Get today's date
        today = datetime.today().date()

        # Define the start of forecast as end of era5 
        # start_time = datetime.now()
        # today = start_time - timedelta(days=6)

        # Add the 'FC' column with True for dates on or after today, and False for dates before today
        modified_quantiles['FC'] = modified_quantiles['date'].apply(lambda x: x.date() >= today)

        modified_quantiles.set_index('date', inplace=True, drop=False)

        # Reset the index so 'date' becomes a column again
        modified_quantiles = modified_quantiles.reset_index(drop=True)


        modified_quantiles.to_csv(filename, sep='\t', index=False)