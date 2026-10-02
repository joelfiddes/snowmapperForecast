#!/usr/bin/env python
"""Rebuild a forecast bundle that a failed run left short or missing.

A forecast bundle is the concatenation of the daily gridded files for its run
date D through D+9, carrying a daily `time` dimension -- exactly what
`upload_to_AWS.py::bundle_nc_files` produces. When a run dies partway, the
bundle is published short (or not at all) and the downstream calculator raises
    "expected to have 10 days forecast ... but we have only N"
for every parameter, on every subsequent check, forever -- it re-reads the same
stale object.

Known damage as of 2026-10-02 (from a full inventory of 59 bundles, Aug-Sep):
    20260809   9 steps   the run_master3 OOM incident
    20260928   missing   cron disabled for the season rollover
    20260929   missing   cron disabled for the season rollover
    20260930   7 steps   rollover day; the sim only reached 10-06

Rebuilding uses the daily grids that exist NOW, so the result is partly
hindsight rather than the forecast actually issued on date D. That is a real
trade-off and the reason this is a deliberate, dated repair rather than
something the pipeline does automatically. It was accepted for these dates
because they are late-summer/early-autumn in Central Asia, where snow is
negligible, and the recurring alarm costs more than the fidelity.
DO NOT use this to paper over a mid-winter failure without thinking again.

Sources:
  --source local   ./spatial/{PARAM}_{YYYYMMDD}.nc   (the sim's own output;
                   only the last ~10 days survive upload_to_AWS's cleanup)
  --source s3      the published daily archive joel-snow-model/{PARAM}/...
                   (complete history, but these are reanalysis-flavoured)

Usage:
  rebuild_forecast_bundle.py --date 20260930 --source local --dry-run
  rebuild_forecast_bundle.py --date 20260930 --source local
  rebuild_forecast_bundle.py --date 20260809 --source s3
"""
import argparse
import os
import tempfile
from datetime import datetime, timedelta

import boto3
import pandas as pd
import xarray as xr

import upload as s3mod

BUCKET = "snow-model-data-source"
ROOT = "joel-snow-model"
PARAMS = ("SWE", "HS", "ROF")
SPATIAL = "./spatial/"


def daily_key(param, day):
    return "%s/%s/%d/%s/%s_%s.nc" % (
        ROOT, param, day.year, day.strftime("%Y%m"), param, day.strftime("%Y%m%d"))


def gather(param, days, source, s3, tmpdir):
    """Local path per day, downloading from the daily archive if needed."""
    paths = []
    for d in days:
        ymd = d.strftime("%Y%m%d")
        if source == "local":
            p = os.path.join(SPATIAL, "%s_%s.nc" % (param, ymd))
            if not os.path.exists(p):
                raise SystemExit("ABORT: missing %s -- cannot build a %d-day bundle"
                                 % (p, len(days)))
        else:
            p = os.path.join(tmpdir, "%s_%s.nc" % (param, ymd))
            if not os.path.exists(p):
                s3.download_file(BUCKET, daily_key(param, d), p)
        paths.append(p)
    return paths


def build(param, run_date, days, paths, out_path):
    """Concatenate daily grids into a bundle, matching bundle_nc_files exactly."""
    datasets = [xr.open_dataset(p) for p in paths]
    combined = xr.concat(datasets, dim="time")
    combined = combined.assign_coords(time=("time", list(days)))
    combined.to_netcdf(out_path)
    for ds in datasets:
        ds.close()
    return out_path


def check_against_reference(candidate, reference, param):
    """Structural comparison with a known-good bundle. Values are not compared."""
    c = xr.open_dataset(candidate, decode_cf=False)
    r = xr.open_dataset(reference, decode_cf=False)
    var = param.lower()
    problems = []
    if var not in c:
        problems.append("variable %r absent (has %s)" % (var, list(c.data_vars)))
    else:
        if c[var].dims != r[var].dims:
            problems.append("dims %s != reference %s" % (c[var].dims, r[var].dims))
        if c[var].dtype != r[var].dtype:
            problems.append("dtype %s != reference %s" % (c[var].dtype, r[var].dtype))
        for k, v in r[var].attrs.items():
            if k in ("GDAL", "history"):
                continue
            if c[var].attrs.get(k) != v:
                problems.append("%s:%s = %r != reference %r"
                                % (var, k, c[var].attrs.get(k), v))
    if "crs" not in c:
        problems.append("crs variable absent")
    for axis in ("lat", "lon"):
        if c[axis].size != r[axis].size:
            problems.append("%s size %d != reference %d" % (axis, c[axis].size, r[axis].size))
        elif not (c[axis].values == r[axis].values).all():
            problems.append("%s values differ from reference" % axis)
    c.close()
    r.close()
    return problems


def main(a):
    run_date = pd.Timestamp(a.date)
    days = [run_date + pd.Timedelta(days=i) for i in range(a.days)]
    print("bundle %s: %d daily steps %s -> %s (source=%s)"
          % (a.date, len(days), days[0].date(), days[-1].date(), a.source))

    s3 = boto3.client("s3")
    cred = boto3.Session().get_credentials().get_frozen_credentials()
    tmpdir = tempfile.mkdtemp(prefix="bundle-")

    # a known-good 10-step bundle to gate the structure against
    ref_local = None
    if a.reference:
        ref_local = os.path.join(tmpdir, "reference.nc")
        s3.download_file(BUCKET, a.reference, ref_local)
        print("  reference: %s" % a.reference)

    for param in PARAMS:
        paths = gather(param, days, a.source, s3, tmpdir)
        out = os.path.join(tmpdir, "%s_%s.nc" % (param, a.date))
        build(param, run_date, days, paths, out)

        ds = xr.open_dataset(out)
        n = ds.sizes.get("time", 0)
        t = pd.DatetimeIndex(ds.time.values)
        ds.close()
        if n != a.days:
            raise SystemExit("ABORT: built %d steps, expected %d" % (n, a.days))

        key = "%s/forecast/%s/%d/%s/%s_%s.nc" % (
            ROOT, param, run_date.year, run_date.strftime("%Y%m"), param, a.date)

        # Compress BEFORE gating: compress_nc (cdo) adds `missing_value`, which
        # every published object carries and the freshly-built file does not.
        # Comparing the pre-compression file against a published reference
        # reports a mismatch that does not exist in the artifact we upload.
        comp = s3mod.compress_nc(out)

        if ref_local:
            problems = check_against_reference(comp, ref_local, param)
            if problems:
                print("  %s STRUCTURE MISMATCH:" % param)
                for p in problems:
                    print("      %s" % p)
                raise SystemExit("ABORT: refusing to publish a bundle that does not "
                                 "match the reference structure")

        if a.dry_run:
            print("  %-4s %d steps %s -> %s  structure OK, DRY RUN, would upload to %s"
                  % (param, n, t.min().date(), t.max().date(), key))
            continue

        ok = s3mod.upload_file(comp, BUCKET, key, cred.access_key, cred.secret_key)
        print("  %-4s %d steps %s -> %s  %s  %s"
              % (param, n, t.min().date(), t.max().date(),
                 "uploaded" if ok else "UPLOAD FAILED", key))
        if not ok:
            raise SystemExit("ABORT: upload failed for %s" % key)

    print("DONE")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="bundle run date YYYYMMDD")
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--source", default="local", choices=["local", "s3"])
    ap.add_argument("--reference", default=None,
                    help="S3 key of a known-good bundle to gate structure against")
    ap.add_argument("--dry-run", action="store_true")
    main(ap.parse_args())
