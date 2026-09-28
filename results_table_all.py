import os
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
import re
from datetime import datetime, timedelta

print("=" * 60)
print("RESULTS_TABLE_ALL.PY - Starting (incremental)")
print("=" * 60)

startTime = datetime.now()
thismonth = startTime.month
thisyear = startTime.year
year = str([thisyear if thismonth in {9, 10, 11, 12} else thisyear - 1][0])
print(f"Processing water year: {year}")

# Load the shapefile containing polygons
shapefile_path = "./master/inputs/basins/basins.shp"
polygons = gpd.read_file(shapefile_path)
directory = "./spatial/"
os.makedirs("./tables", exist_ok=True)


def extract_mean_values(merged_raster, polygons):
    """Area mean of the raster inside each polygon.

    `crop=True` crops to the polygon's BOUNDING BOX, so much of what comes back
    lies outside the polygon itself. `filled=False` keeps those cells masked.
    Under the default `filled=True`, rasterio fills them with the raster's
    nodata value -- and these rasters declare `nodata: None`, so it falls back
    to **0**, which `np.nanmean` then averages in as real data.

    Every basin mean was therefore diluted by roughly bbox-area/polygon-area.
    Measured across 292 basins: median **1.965x** too low, mean 2.054x, up to
    3.95x (basin CODE 17165 read 310.77 mm against a true polygon mean of
    601.60 mm). Fixed at the 2026/27 season rollover, where the table series
    restarts from zero so the correction introduces no mid-season step change.

    `masked_invalid` additionally drops NaN cells *inside* the polygon, which
    the old `nanmean` handled and which must keep working. Note we do NOT treat
    zeros as missing -- genuine in-polygon zeros are real data (601.60 true vs
    604.27 if zeros were discarded).
    """
    mean_values = []
    dates = []
    for idx, geom in enumerate(polygons.geometry):
        masked, _ = mask(merged_raster, [geom], crop=True, filled=False)
        data = np.ma.masked_invalid(masked)
        # count() is the number of UNMASKED cells; 0 means the polygon covers
        # no valid data, where .mean() would return the np.ma.masked singleton
        mean_value = np.nan if data.count() == 0 else float(data.mean())
        mean_values.append(mean_value)
        date = "YYYY-MM-DD"
        dates.append(date)
    return mean_values, dates


def natural_sort(l):
    def convert(text): return int(text) if text.isdigit() else text.lower()
    def alphanum_key(key): return [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


def convert_to_timestamp(date_str):
    year, doy = map(int, date_str.split('_'))
    base_date = datetime(year, 9, 1)
    target_date = base_date + timedelta(days=doy)
    return target_date


def get_processed_count(csv_path):
    """Return number of rows already in the CSV, or 0 if it doesn't exist."""
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        return len(df)
    return 0


def process_block(label, glob_pattern, col_key, csv_path, is_basin):
    """Process a single variable block incrementally.

    For basin tables: columns from polygons[col_key], with groupby averaging.
    For catchment tables: columns from polygons[col_key], no groupby.
    """
    columns = [str(idx) for idx in list(polygons[col_key])]
    a = glob.glob(glob_pattern)
    file_list = natural_sort(a)
    total_files = len(file_list)

    # Load existing results if available
    already_processed = 0
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path, parse_dates=['Date'])
        already_processed = len(existing_df)

    if already_processed >= total_files:
        print(f"  {label} - Already up to date ({already_processed} rows, {total_files} files). Skipping.")
        return

    new_files = file_list[already_processed:]
    print(f"  {label} - {total_files} files total, {already_processed} already done, processing {len(new_files)} new")

    # Process only new files
    results_df = pd.DataFrame(columns=['Date'] + columns)
    for i, filename in enumerate(new_files):
        print(f"  {label} - Processing {already_processed + i + 1}/{total_files}: {os.path.basename(filename)}")

        date1 = filename.split("_")[3]
        day = filename.split("_")[4].split(".")[0]
        date = date1 + "_" + day
        timestamp = convert_to_timestamp(date)

        merged_raster = rasterio.open(filename)
        mean_values, _ = extract_mean_values(merged_raster, polygons)
        merged_raster.close()

        results_df.loc[len(results_df)] = [timestamp] + mean_values

    if is_basin:
        # Basin tables need groupby averaging before save
        # Rebuild full dataset: reload existing raw data or recompute from scratch
        # Since basin CSVs store the averaged result, we need to keep raw data too
        # Strategy: keep a raw CSV alongside, append to it, then compute averaged output
        raw_csv = csv_path.replace(".csv", "_raw.csv")
        if os.path.exists(raw_csv):
            existing_raw = pd.read_csv(raw_csv, parse_dates=["Date"])
            # read_csv mangles duplicate column names (.1, .2 suffixes) - align new data to match
            results_df.columns = existing_raw.columns
            full_df = pd.concat([existing_raw, results_df], ignore_index=True)
        else:
            # First time with incremental - need to rebuild from the existing averaged CSV
            # or process all files. Since we may not have raw data, reprocess all for basin.
            if already_processed > 0:
                # We don't have raw data from before, so reprocess all basin files
                print(f"  {label} - No raw data found, reprocessing all {total_files} files for basin table")
                full_df = pd.DataFrame(columns=['Date'] + columns)
                for filename in file_list:
                    date1 = filename.split("_")[3]
                    day = filename.split("_")[4].split(".")[0]
                    date = date1 + "_" + day
                    timestamp = convert_to_timestamp(date)
                    merged_raster = rasterio.open(filename)
                    mean_values, _ = extract_mean_values(merged_raster, polygons)
                    merged_raster.close()
                    full_df.loc[len(full_df)] = [timestamp] + mean_values
            else:
                full_df = results_df

        # Save raw data for future incremental runs
        full_df.to_csv(raw_csv, index=False)

        # Compute basin averages
        date_column = full_df['Date']
        df_no_date = full_df.drop(columns=['Date'])
        # Strip .N suffixes from mangled duplicate column names for proper grouping
        original_names = [re.sub(r"\.\d+$", "", col) for col in df_no_date.columns]
        averages = df_no_date.T.groupby(original_names).mean().T
        out = pd.concat([date_column.reset_index(drop=True), averages.reset_index(drop=True)], axis=1)
        out.to_csv(csv_path, index=False)
    else:
        # Catchment tables - just append
        if already_processed > 0:
            existing_df = pd.read_csv(csv_path, parse_dates=['Date'])
            full_df = pd.concat([existing_df, results_df], ignore_index=True)
        else:
            full_df = results_df
        full_df.to_csv(csv_path, index=False)

    print(f"  {label} - Saved {csv_path}")


# ===============================================================================
# Basin tables (REGION columns, with groupby averaging)
# ===============================================================================
print("\n[1/6] Basin SWE")
process_block("Basin SWE",
              directory + "swe_merged_reprojected_" + year + "*",
              "REGION", "./tables/swe_basin_mean_values_table.csv", is_basin=True)

print("\n[2/6] Basin HS")
process_block("Basin HS",
              directory + "hs_merged_reprojected_" + year + "*",
              "REGION", "./tables/hs_basin_mean_values_table.csv", is_basin=True)

print("\n[3/6] Basin ROF")
process_block("Basin ROF",
              directory + "ROF_merged_reprojected_" + year + "*",
              "REGION", "./tables/rof_basin_mean_values_table.csv", is_basin=True)

# ===============================================================================
# Catchment tables (CODE columns, no groupby)
# ===============================================================================
print("\n[4/6] Catchment SWE")
process_block("Catchment SWE",
              directory + "swe_merged_reprojected_" + year + "*",
              "CODE", "./tables/swe_mean_values_table.csv", is_basin=False)

print("\n[5/6] Catchment HS")
process_block("Catchment HS",
              directory + "hs_merged_reprojected_" + year + "*",
              "CODE", "./tables/hs_mean_values_table.csv", is_basin=False)

print("\n[6/6] Catchment ROF")
process_block("Catchment ROF",
              directory + "ROF_merged_reprojected_" + year + "*",
              "CODE", "./tables/rof_mean_values_table.csv", is_basin=False)

print(f"\nDone in {datetime.now() - startTime}")
