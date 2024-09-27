import pandas as pd
import numpy as np

# Define the start and end dates (this of course changes by year use my old code to make more generic?)


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
        
        
        # # Reorder the columns
        # column_order = ['date', 'Q5_SWE', 'Q5_HS', 'Q50_SWE', 'Q50_HS', 'Q95_SWE', 'Q95_HS']
        # modified_quantiles = modified_quantiles.reindex(columns=column_order)
        
        # # Reset the index to dates
        # modified_quantiles.reset_index(inplace=True)

        # # Convert the 'Date' column to datetime format
        # modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # # Set the 'Date' column as the index
        # modified_quantiles.set_index('date', inplace=True, drop=False)

        # # Write the quantiles to a new file
        # filename = f"./newdomain/tables/{catchment}_current.txt"
        # modified_quantiles = modified_quantiles.drop(columns=['index'])
        # modified_quantiles.to_csv(filename, sep='\t',index=False)

        

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



        # # Reorder the columns
        # column_order = ['date', 'Q5_SWE', 'Q5_HS', 'Q50_SWE', 'Q50_HS', 'Q95_SWE', 'Q95_HS']
        # modified_quantiles = modified_quantiles.reindex(columns=column_order)
        
        # # Reset the index to dates
        # modified_quantiles.reset_index(inplace=True)

        # # Convert the 'Date' column to datetime format
        # modified_quantiles['date'] = pd.to_datetime(modified_quantiles['date'])

        # # Set the 'Date' column as the index
        # modified_quantiles.set_index('date', inplace=True, drop=False)

        # # Calculate the mean of '2023-12-31' and '2024-01-02' for each column
        # # Calculate the mean of adjacent dates for all relevant columns
        # # replacement_values = modified_quantiles.loc[['2023-12-31', '2024-01-02'], ['Q5_SWE', 'Q5_HS', 'Q50_SWE', 'Q50_HS', 'Q95_SWE', 'Q95_HS']].mean()

        # # Replace the values for '2024-01-01' with the calculated mean
        # # modified_quantiles.loc['2024-01-01', ['Q5_SWE', 'Q5_HS', 'Q50_SWE', 'Q50_HS', 'Q95_SWE', 'Q95_HS']] = replacement_values


        # # Write the quantiles to a new file
        # filename = f"./newdomain/tables/{catchment}_current.txt"
        # modified_quantiles = modified_quantiles.drop(columns=['index'])
        # modified_quantiles.to_csv(filename, sep='\t',index=False)




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
        modified_quantiles.to_csv(filename, sep='\t', index=False)
