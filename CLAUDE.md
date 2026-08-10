# snowmapperForecast

Python codebase for the Central Asia SnowMapper operational pipeline. Runs daily on AWS EC2 to produce snow reanalysis (ERA5-driven) and IFS forecast products, then uploads to S3 for the dashboard.

## Architecture

The pipeline is orchestrated by a bash script (`snowmapper_2026/run_2000.sh`) that calls these modules in sequence:

### Pipeline steps (in order)

1. **`fetch_ifs_forecast.py`** — Downloads latest IFS forecast data from ECMWF
2. **`run_master3.py`** — Runs the master simulation (TopoPyScale downscaling) in `./master/`
3. **`run_latest.py`** — Runs FSM snow model for the latest timestep in `./D2000/`
4. **`concat_fsm.py`** — Concatenates FSM output files in `./D2000/`
5. **`make_netcdf_files.py`** — Converts FSM outputs to NetCDF (SWE, HS, ROF) in `./D2000/outputs/`
6. **`merge_reproj_single_domain.py`** — Reprojects NetCDFs to EPSG:4326, writes TIFs and spatial NetCDFs to `./spatial/`
7. **`results_table_all.py`** — Zonal stats: extracts basin/catchment mean values from TIFs, writes CSVs to `./tables/`. **Incremental** — skips already-processed files.
8. **`zonal_stats.py`** — Additional zonal statistics
9. **`scp` to dashboard** — Copies `tables/*.txt` to the MCASS dashboard EC2
10. **`upload_to_AWS.py`** — Uploads daily reanalysis + 10-day forecast bundles to S3

### Support modules

- **`upload.py`** — S3 upload helpers, compression, path generation. Bucket: `snow-model-data-source`, prefix: `joel-snow-model/`
- **`aws_mail.py`** — Email notifications via Gmail SMTP. Called by `run_2000.sh` on pipeline failure.
- **`upload_to_AWS_offline.py`** — Manual backfill upload for a specific date: `python upload_to_AWS_offline.py 20260305`
- **`upload_to_AWS_offline_Forecast.py`** — Manual backfill for forecast bundles
- **`setup_sim.py`** — Initial domain setup (run once)
- **`run_first.py`** — First-time FSM run (full history, not used in daily ops)

### Inactive/legacy

- `run_master.py`, `run_master2.py`, `run_master2_test.py` — older master versions, replaced by `run_master3.py`
- `merge_reproj.py` — multi-domain merge, replaced by `merge_reproj_single_domain.py`
- `run_current_month.py`, `run_last_month.py`, `run_forecast.py` — not used in current pipeline
- `getModis.py`, `modisProcess.py` — MODIS processing, not part of daily pipeline
- `handleNewNetcdfFormat.py` — format migration helper

## S3 structure

Bucket: `snow-model-data-source`

```
joel-snow-model/
  {SWE,HS,ROF}/{year}/{yearmonth}/{PARAM}_{YYYYMMDD}.nc    # daily reanalysis
  forecast/{SWE,HS,ROF}/{year}/{yearmonth}/{PARAM}_{YYYYMMDD}.nc  # 10-day bundles
```

## Key patterns

- Water year runs Sep-Aug. Year calculation: `thisyear` if month >= 9, else `thisyear - 1`
- ERA5 reanalysis has ~6 day lag (`fetch_era5.return_last_fullday()`)
- TIF files use index-based naming: `swe_merged_reprojected_{year}_{idx}.tif` where idx = days since Sep 1
- `results_table_all.py` is incremental — it counts rows in existing CSVs and skips that many TIF files
- Spatial NetCDFs written via rasterio NetCDF driver, then attributes set via netCDF4

## Dependencies

- TopoPyScale (downscaling, ERA5 fetching)
- FSM (Fortran Snow Model, compiled binary in D2000/FSM/)
- rasterio, rioxarray, xarray, geopandas, rasterstats
- boto3 (S3 uploads)
- Conda env: `downscaling`
