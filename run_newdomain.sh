#!/usr/bin/env bash
conda activate downscaling
python run_master.py  "./newdomain/master/"
python fetch_ifs_forecast.py "./newdomain/"
#python setup_sim.py ./newdomain/D4

# if month is not september then:
#python run_last_month.py ./newdomain/D1   & # 2> error.txt 1> output.txt Doesnt work in september as august is assigned to end of season ie if sept 2024 looks for Aug2025 data not Aug2024
python run_current_month.py ./newdomain/D4   # 2> error.txt 1> output.txt
python run_forecast.py ./newdomain/D4    # 2> error.txt 1> output.txt
python concat_fsm.py ./newdomain/D4
python make_netcdf_files.py ./newdomain/D4

#python merge_reproj_new.py --no-upload
python merge_reproj_single_domain.py ./newdomain/ False
python results_table_all.py
python zonal_stats.py

cp ./newdomain/tables/*.txt /home/joel/sim/TPS_2024/MCASS/data

# spatial_func
