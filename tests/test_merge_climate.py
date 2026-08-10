"""Synthetic check of merge_climate_files3: ordering, gap fill, priority, tail.

Builds a tiny fake season on the same file-naming conventions the pipeline uses,
then asserts the merged output is what we expect. Stubs TopoPyScale/munch so the
real run_master3.py can be imported on the Mac.
"""
import shutil
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

# --- stub the server-only imports so run_master3 can be imported here ---
for name in ("TopoPyScale", "TopoPyScale.topoclass", "munch"):
    mod = types.ModuleType(name)
    sys.modules.setdefault(name, mod)
sys.modules["TopoPyScale"].topoclass = sys.modules["TopoPyScale.topoclass"]
sys.modules["munch"].DefaultMunch = object

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import run_master3 as rm

LAT = np.array([44.35, 44.10, 43.85])
LON = np.array([59.60, 59.85])
LEVELS = np.array([1000, 850, 700, 500])


def make(path, times, value, with_level=True, lat=LAT, lon=LON, levels=LEVELS):
    """One netCDF file whose data equals `value` everywhere (so provenance is traceable)."""
    if with_level:
        shape = (len(times), len(levels), len(lat), len(lon))
        dims = ("time", "level", "latitude", "longitude")
        coords = {"time": times, "level": levels, "latitude": lat, "longitude": lon}
    else:
        shape = (len(times), len(lat), len(lon))
        dims = ("time", "latitude", "longitude")
        coords = {"time": times, "latitude": lat, "longitude": lon}
    ds = xr.Dataset({"t": (dims, np.full(shape, value, dtype="float32"))}, coords=coords)
    ds.to_netcdf(path)


def hours(day, n=24, start=0):
    d = pd.Timestamp(day)
    return pd.date_range(d + pd.Timedelta(hours=start), periods=n, freq="h")


def run_case(tmp, label, with_level=True):
    tmp.mkdir(parents=True, exist_ok=True)
    for f in tmp.glob("*.nc"):
        f.unlink()

    # ERA5 days 01,02,04,05 -- day 03 missing entirely (the 2026-08-03 scenario).
    # Day 05 is short: only 20 of 24 hours.
    for day, val in [("2026-08-01", 1.0), ("2026-08-02", 2.0), ("2026-08-04", 4.0)]:
        make(tmp / f"SURF_{day.replace('-', '')}.nc", hours(day), val, with_level)
    make(tmp / "SURF_20260805.nc", hours("2026-08-05", n=20), 5.0, with_level)

    # Daily forecast fallbacks: one for the missing day 03, one for day 02 that
    # must be IGNORED because ERA5 already covers it (priority check).
    make(tmp / "SURF_FC_2026-08-03.nc", hours("2026-08-03"), 30.0, with_level)
    make(tmp / "SURF_FC_2026-08-02.nc", hours("2026-08-02"), 20.0, with_level)

    # Continuous forecast: covers the tail of day 05 plus days 06-07.
    cont = pd.date_range("2026-08-05T20:00", "2026-08-07T23:00", freq="h")
    make(tmp / "SURF_FC.nc", cont, 99.0, with_level)

    out = tmp / "merged.nc"
    rm.merge_climate_files3(str(tmp), "SURF", str(out))
    rm.validate_merged_file(str(out), "SURF")

    ds = xr.open_dataset(out)
    t = pd.DatetimeIndex(pd.to_datetime(ds.time.values))
    vals = ds.t.isel(latitude=0, longitude=0)
    if with_level:
        vals = vals.isel(level=0)
    vals = vals.values

    def at(ts):
        return float(vals[t.get_loc(pd.Timestamp(ts))])

    checks = [
        ("monotonic time", t.is_monotonic_increasing),
        ("no duplicate times", not t.has_duplicates),
        ("contiguous hourly", len(pd.date_range(t.min(), t.max(), freq="h").difference(t)) == 0),
        ("starts 08-01T00", t.min() == pd.Timestamp("2026-08-01T00:00")),
        ("ends 08-07T23", t.max() == pd.Timestamp("2026-08-07T23:00")),
        ("era5 day01 kept", at("2026-08-01T05:00") == 1.0),
        ("era5 beats daily FC on day02", at("2026-08-02T05:00") == 2.0),
        ("day03 gap filled from daily FC", at("2026-08-03T05:00") == 30.0),
        ("era5 day04 kept", at("2026-08-04T05:00") == 4.0),
        ("short day05 era5 part kept", at("2026-08-05T05:00") == 5.0),
        ("short day05 patched from cont FC", at("2026-08-05T21:00") == 99.0),
        ("tail from cont FC", at("2026-08-06T12:00") == 99.0),
    ]
    ds.close()

    print(f"\n--- {label} ---")
    ok = True
    for name, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        ok &= bool(passed)
    return ok


def run_level_mismatch(tmp):
    """A forecast with the wrong pressure levels must raise, not silently mis-assign."""
    tmp.mkdir(parents=True, exist_ok=True)
    for f in tmp.glob("*.nc"):
        f.unlink()
    make(tmp / "PLEV_20260801.nc", hours("2026-08-01"), 1.0)
    make(tmp / "PLEV_FC_2026-08-02.nc", hours("2026-08-02"), 2.0,
         levels=np.array([1000, 925, 850, 700]))
    make(tmp / "PLEV_20260803.nc", hours("2026-08-03"), 3.0)
    make(tmp / "PLEV_FC.nc", hours("2026-08-04"), 9.0)
    try:
        rm.merge_climate_files3(str(tmp), "PLEV", str(tmp / "m.nc"))
    except ValueError as exc:
        print(f"\n--- level mismatch ---\n  PASS  raised: {str(exc)[:70]}...")
        return True
    print("\n--- level mismatch ---\n  FAIL  no error raised")
    return False


def run_corrupt_detection(tmp):
    """validate_merged_file must reject the exact 2026-08-09 artefact."""
    tmp.mkdir(parents=True, exist_ok=True)
    bad = tmp / "corrupt.nc"
    t = hours("2026-08-01")
    ds = xr.Dataset(
        {"t": (("time", "latitude", "longitude"),
               np.full((len(t), 3, 2), np.nan, dtype="float32"))},
        coords={"time": t, "latitude": LAT, "longitude": np.full(2, np.nan)},
    )
    ds.to_netcdf(bad)
    try:
        rm.validate_merged_file(str(bad), "PLEV")
    except RuntimeError as exc:
        print(f"\n--- corrupt file detection ---\n  PASS  raised: {str(exc)[:70]}...")
        return True
    print("\n--- corrupt file detection ---\n  FAIL  corrupt file accepted")
    return False


def run_encoding_check(tmp):
    """Output must be uncompressed even when the source dailies are compressed.

    Opening the per-day files individually carries their deflate settings into
    the result unless the write pins encoding explicitly. The pre-fix pipeline
    wrote uncompressed files (byte-exact: SURF 953,985,257 B = 7 vars x 8449 x
    48 x 84 x 4 B), and run_latest re-reads the result once per point across
    2000 points, so a decompress on every read would be a real regression.
    """
    tmp.mkdir(parents=True, exist_ok=True)
    for f in tmp.glob("*.nc"):
        f.unlink()

    for day, val in [("2026-08-01", 1.0), ("2026-08-02", 2.0)]:
        t = hours(day)
        xr.Dataset(
            {"t2m": (("time", "latitude", "longitude"),
                     np.full((len(t), len(LAT), len(LON)), val, dtype="float32"))},
            coords={"time": t, "latitude": LAT, "longitude": LON},
        ).to_netcdf(
            tmp / f"SURF_{day.replace('-', '')}.nc",
            encoding={"t2m": {"zlib": True, "complevel": 1, "shuffle": True}},
        )
    make(tmp / "SURF_FC.nc", hours("2026-08-03"), 9.0, with_level=False)

    out = tmp / "merged.nc"
    rm.merge_climate_files3(str(tmp), "SURF", str(out))

    def hdr_of(p):
        return subprocess.run(["ncdump", "-h", "-s", str(p)],
                              capture_output=True, text=True).stdout

    src_compressed = "_DeflateLevel" in hdr_of(tmp / "SURF_20260801.nc")
    hdr = hdr_of(out)
    bare = 72 * len(LAT) * len(LON) * 4

    checks = [
        ("source dailies really are compressed", src_compressed),
        ("output is NOT compressed", "_DeflateLevel" not in hdr),
        ("output is contiguous", 't2m:_Storage = "contiguous"' in hdr),
        ("output holds full uncompressed data", out.stat().st_size >= bare),
    ]
    print("\n--- output encoding ---")
    ok = True
    for name, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        ok &= bool(passed)
    return ok


if __name__ == "__main__":
    base = Path(tempfile.mkdtemp(prefix="mergetest-"))
    results = [
        run_case(base / "plev", "with level dim (PLEV-like)", with_level=True),
        run_case(base / "surf", "no level dim (SURF-like)", with_level=False),
        run_level_mismatch(base / "levels"),
        run_corrupt_detection(base / "corrupt"),
        run_encoding_check(base / "encoding"),
    ]
    shutil.rmtree(base, ignore_errors=True)
    print("\n" + ("ALL PASS" if all(results) else "FAILURES PRESENT"))
    sys.exit(0 if all(results) else 1)
