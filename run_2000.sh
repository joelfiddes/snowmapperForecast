#!/usr/bin/env bash
# Daily SnowMapper pipeline for domain D2000.
#
# Tracked in git so it deploys like everything else. The path the cron invokes,
# /home/ubuntu/sim/snowmapper_2027/run_2000.sh, is a symlink to this file:
#   50 15 * * * conda activate downscaling; ./sim/snowmapper_2027/run_2000.sh > ./sim/snowmapper_2027/run.log 2>&1
# Note the single '>': run.log holds the most recent run only.

conda activate downscaling

# Season directory, named for the water-year END year (WY2026-27 -> _2027).
# Overridable so the path is in one place when the season turns.
WDIR="${WDIR:-/home/ubuntu/sim/snowmapper_2027}"
SRC=/home/ubuntu/src/snowmapperForecast
MAIL=$SRC/aws_mail.py
cd $WDIR

# Capture start time
start_date_time="`date +%Y%m%d%H%M%S`"
start_seconds=`date +%s`
echo "Start time: $start_date_time"

FAILED=""
ABORTED=""

# run_step     <label> <cmd...>  -- record a failure, carry on
# run_critical <label> <cmd...>  -- record a failure AND skip everything after it
#
# The abort is the point. Until 2026-08-10 every step ran unconditionally, so
# when run_master3 was OOM-killed and run_latest died with it, the publish steps
# still ran against the previous day's fields and pushed them to S3 and the
# MCASS dashboard stamped with today's date. A run that fails upstream must
# leave the published product alone rather than restate yesterday as today.
run_step() {
    local label="$1"; shift
    if [ -n "$ABORTED" ]; then
        echo "SKIPPED $label (chain aborted after $ABORTED failed)"
        return 0
    fi
    echo "----- $label -----"
    if ! "$@"; then
        echo "FAILED $label"
        FAILED="$FAILED $label"
        return 1
    fi
    return 0
}

run_critical() {
    local label="$1"
    if ! run_step "$@"; then
        if [ -z "$ABORTED" ]; then
            ABORTED="$label"
        fi
    fi
    return 0
}

# Forecast fetch is not fatal: the merge falls back to the forecast file already
# on disk, and run_master3 now reports the tail extent it actually used.
run_step     fetch_ifs_forecast python $SRC/fetch_ifs_forecast.py

# Forcing + model chain. Any failure here invalidates today's product.
run_critical run_master3       python $SRC/run_master3.py "./master/"
#run_critical run_first        python $SRC/run_first.py ./D2000
run_critical run_latest        python $SRC/run_latest.py ./D2000
run_critical concat_fsm        python $SRC/concat_fsm.py ./D2000
run_critical make_netcdf_files python $SRC/make_netcdf_files.py ./D2000
run_critical merge_reproj      python $SRC/merge_reproj_single_domain.py "./" "D2000" False

# Reporting + publishing. Independent of each other, so these stay non-fatal.
run_step     results_table_all python $SRC/results_table_all.py
run_step     zonal_stats       python $SRC/zonal_stats.py
#cp ./tables/*.txt /home/ubuntu/sim/TPS_2024/MCASS/data
run_step     scp_dashboard     scp -i ~/.ssh/swe_dashboard tables/*.txt ec2-user@13.49.227.116:/home/ec2-user/MCASS/data/
run_step     upload_to_AWS     python $SRC/upload_to_AWS.py

# Capture end time
end_date_time="`date +%Y%m%d%H%M%S`"
end_seconds=`date +%s`
runtime_seconds=$((end_seconds - start_seconds))
hours=$((runtime_seconds / 3600))
minutes=$(((runtime_seconds % 3600) / 60))
seconds=$((runtime_seconds % 60))
runtime=$(printf "%02d:%02d:%02d" $hours $minutes $seconds)

if [ -n "$FAILED" ]; then
    echo "FAILED steps:$FAILED"
    if [ -n "$ABORTED" ]; then
        python $MAIL "SnowMapper Pipeline FAILED (nothing published)" \
"Failed steps:$FAILED

Chain aborted at: $ABORTED -- every later step was skipped, so S3 and the MCASS
dashboard still hold the PREVIOUS run's product. Nothing stale was published
under today's date.

Runtime: $runtime
Log: $WDIR/run.log"
    else
        python $MAIL "SnowMapper Pipeline FAILED (forcing OK, publishing incomplete)" \
"Failed steps:$FAILED

The forcing and model chain completed, so today's fields are good; the failures
above are in the reporting/publishing steps only.

Runtime: $runtime
Log: $WDIR/run.log"
    fi
else
    echo "run complete in $runtime"
fi

echo "End time: $end_date_time"
printf "Total runtime: %s\n" $runtime

if [ -n "$FAILED" ]; then
    exit 1
fi
