# Annual season rollover — legacy CA snowmapper (aws 13.50.55.27)

**EXECUTED 2026-09-30. This was the FINAL rollover for this code** — Joel confirmed the legacy
system retires after WY2026-27, with TPS2 taking over the datagateway feed. Kept as the operating
record for the remaining season and for the cutover, not as a procedure anyone runs again.

Outcome: season reseeded 2026-09-01, 36 days to 2026-10-06; 108 rasters all tagged 2026; 300 basin
tables 2026-09-01 → 2026-10-06 with **zero** rows dated 2027 (they had run to 2027-10-06);
`*_previous.txt` rebuilt to 2025-26 (was 2024-25); S3 and the MCASS viewer both republished; cron
re-enabled. Timings: `run_master3` 7m/1.35 GB, `run_first` 3h17m/3.39 GB, the rest 25m/1.62 GB —
all of it comfortably inside 2 GB except `run_first`.

This procedure existed only as two lines in CLAUDE.md, which is a large part of why the
2026 rollover slipped four weeks and started mis-dating the basin tables.

---

## Why it has to happen, and why 1 September

The water year in this code is **Sep–Aug**, and the date arithmetic is hard-coded to it:

```python
merge_reproj_single_domain.py:36   year = thisyear if thismonth in {9,10,11,12} else thisyear-1
merge_reproj_single_domain.py:106  f'swe_merged_reprojected_{year}_{time_idx}.tif'   # time_idx = index into the FSM series
results_table_all.py:48            base_date = datetime(year, 9, 1) + timedelta(days=doy)
```

**FSM index 0 ⇒ 1 September of the label year.** The sim start and the label must agree or every
table row shifts.

Current breakage: the label flipped to 2026 on 2026-09-01 but the sim still starts 2025-09-01, so
index 0 is read as 2026-09-01 instead of 2025-09-01 and `tables/*_current.txt` now runs
**2025-09-01 → 2027-10-06** — a year into the future.

**A 1 October start would reintroduce the same bug at 30 days** unless `base_date` is also changed.
Start the sim on **2026-09-01** to stay consistent with the code and with last season (whose tables
begin 2025-09-01).

The **gridded S3 product is not affected** either way — it is dated from the FSM datetime index, not
this arithmetic (`SWE_20260921.nc` is correct today).

## Why `run_first.py`, not `run_latest.py`

From `concat_fsm.py`:

```
# first run goes to sim archive, while sim_latest is empty so nothing is merged
# all subsequent runs write to sim_latest then merged with the extending sim_archive files
```

`run_latest.py` only fills `sim_latest` over a **7-day** lookback (`run_latest.py:139`,
`first_timestamp = start_time - timedelta(days=7)`) and `concat_fsm` merges that into the extending
`sim_archive`. It cannot seed a season. `run_first.py` is what creates the archive.

At this point in the season `run_first` simulates only ~5 weeks (2026-09-01 → the ERA5/forecast
edge), so it is a short job, not a full-year one.

**Note**: restarting the season zeroes the snowpack at 1 Sept, discarding carry-over from the
previous year. That is what every previous season did, so it is consistent — but it is a real choice.

---

## Sequence

Run from `/home/ubuntu/sim/snowmapper_2026` unless stated. Env: `conda activate downscaling`.

### 0. Pick your moment

The box is only 16 GB between ~15:34 and ~21:34 UTC and the daily cron fires at 15:50. Start early
in that window, with the cron disabled, so nothing collides and nothing is killed by the downsize.

### 1. Disable the cron first

```bash
crontab -l > ~/crontab.backup.$(date +%Y%m%d)
crontab -l | sed 's|^50 15 .*run_2000.sh|#&|' | crontab -
crontab -l | grep run_2000            # confirm it is commented
```

### 2+4. Set the old season aside by RENAMING, not deleting

Measured 2026-09-28: `spatial/*.tif` is **20 G**, `sim_latest/outputs` 6.3 G,
`sim_archive/outputs` 2.8 G, `fsm_sims` 71 M, `tables` 24 M — against 142 G free.

Renaming is instant, costs no disk copy, and makes rollback a second `mv` instead of an untar.
Tar only the two small, scientifically valuable sets as belt-and-braces. Delete the renamed
directories only once the new season is verified at step 6.

```bash
cd /home/ubuntu/sim/snowmapper_2026
OLD=WY2025-26
mkdir -p ~/season_archive/$OLD

# small + valuable: copy properly
cp -a tables            ~/season_archive/$OLD/
cp -a D2000/fsm_sims    ~/season_archive/$OLD/
cp -a D2000/config.yml  ~/season_archive/$OLD/D2000_config.yml
cp -a master/config.yml ~/season_archive/$OLD/master_config.yml

# bulk: rename in place, no copy
mkdir -p spatial_$OLD && mv spatial/*.tif spatial_$OLD/
mv D2000/fsm_sims            D2000/fsm_sims.$OLD            && mkdir D2000/fsm_sims
mv D2000/sim_archive/outputs D2000/sim_archive/outputs.$OLD && mkdir D2000/sim_archive/outputs
mv D2000/sim_latest/outputs  D2000/sim_latest/outputs.$OLD  && mkdir D2000/sim_latest/outputs

ls spatial/*.tif 2>/dev/null | wc -l   # expect 0
ls D2000/fsm_sims/ | wc -l             # expect 0
df -h /
```

Leave `spatial/*.nc` alone — those are the daily gridded files on a 10-day rotation, cleaned by
`upload_to_AWS.py`. The season's gridded product is already on S3 under `joel-snow-model/` and is
unaffected by the rollover.

### 3. Point both configs at the new season

```bash
cd /home/ubuntu/sim/snowmapper_2026
sed -i -E 's|^([[:space:]]*)start:.*|\1start: 2026-09-01|' D2000/config.yml master/config.yml
grep -nE '^[[:space:]]*(start|end):' D2000/config.yml master/config.yml
```

Expect `    start: 2026-09-01` in both, indentation intact. `end:` is overwritten at runtime by
`update_config_paths()`, so its stale 2025-12-31 value does not matter — leave it.

**Do not use `\g<1>`** — that is Python regex syntax, not sed. Dry-run on the live configs showed
sed treats it as literal text and writes `g<1>2026-09-01`, destroying the `start:` key, **and still
exits 0**. Verified 2026-09-28: each config has exactly one `start:` line, and after the corrected
substitution the file still parses with `project.start = datetime.date(2026, 9, 1)` as a `date`
object — which matters, because `update_config_paths()` calls `.replace(year=…)` on it.

### 4b. Do NOT deploy the zonal-stats fix

**Deliberately skipped** — see "The zonal-stats dilution: deliberately left alone" below.
`results_table_all.py` on the box stays as it is. Nothing to do at this step.

### 5. Seed the new season

```bash
cd /home/ubuntu/sim/snowmapper_2026
python /home/ubuntu/src/snowmapperForecast/run_master3.py ./master/ 2>&1 | tee rollover_master.log
python /home/ubuntu/src/snowmapperForecast/run_first.py  ./D2000   2>&1 | tee rollover_first.log
```

`run_master3` first so `master/inputs/climate/SURF_final_merged_output.nc` is current —
`run_first` reads its last full day as the simulation end.

### 6. Verify BEFORE re-enabling the cron

```bash
cd /home/ubuntu/sim/snowmapper_2026
head -1 D2000/fsm_sims/sim_FSM_pt_0000.txt        # expect 2026 9 1
wc -l < D2000/fsm_sims/sim_FSM_pt_0000.txt        # expect ~ days since 1 Sept (+ forecast)
```

Then run the rest of the chain by hand and check the dates it produces:

```bash
python /home/ubuntu/src/snowmapperForecast/concat_fsm.py ./D2000      # ALSO RUNS FSM
python /home/ubuntu/src/snowmapperForecast/make_netcdf_files.py ./D2000
python /home/ubuntu/src/snowmapperForecast/merge_reproj_single_domain.py "./" "D2000" False
python /home/ubuntu/src/snowmapperForecast/results_table_all.py       # writes only the CSVs
python /home/ubuntu/src/snowmapperForecast/zonal_stats.py             # writes the *_current.txt

ls spatial/*.tif | sed -E 's/.*_reprojected_([0-9]{4})_.*/\1/' | sort -u   # expect 2026 ONLY
sed -n '2p' tables/ISSYKUL_current.txt | cut -f1                           # expect 2026-09-01
tail -1     tables/ISSYKUL_current.txt | cut -f1                           # expect ~today, NOT 2027
```

**Two traps, both hit on 2026-09-30:**

- **`zonal_stats.py` is not optional.** `results_table_all.py` writes only the intermediate
  `tables/*.csv`; the `*_current.txt` files the viewer reads come from `zonal_stats.py`. Omitting it
  gives `Exit status: 0` and **zero tables** — success by exit code, nothing produced.
- **`concat_fsm` is what executes FSM** (`concat_fsm.py:217 simulate_fsm` →
  `sim.fsm_sim(..., "./FSM")`). `run_first` only writes the FSM *driving* files. So `D2000/fsm_sims`
  is empty until this step runs, and gating on it beforehand deadlocks. Gate on
  `D2000/sim_archive/outputs/FSM_pt_*.txt == 2000` instead.

**The last two checks are the whole point.** First row 2026-09-01 and last row near today means the
index origin and the label agree again. If the last row is a year out, stop — the sim start and the
label have desynchronised and re-enabling the cron will keep publishing bad dates.

### 6b. Rebuild `*_previous.txt` — a manual step with no producer in the repo

The MCASS viewer reads **three** files per basin, and only one of them is produced by the pipeline:

| file | count | produced by |
|---|---|---|
| `*_current.txt` | 300 | `zonal_stats.py`, daily |
| `*_previous.txt` | 300 | **nothing — written by hand at rollover** |
| `*_climate.txt` | 301 | **nothing — last written 2024-10-23** |

Nothing in this repo greps for `_previous` or `_climate`. Before 2026-09-30 the "previous" line had
sat untouched since 2025-10-02, so it still held **2024-25** while current showed 2026-27 — the
viewer was comparing against a season two years old. Miss this and the rollover looks complete but
the comparison is silently wrong.

Rebuild it from the archived season, taking the full water year Sept 1 → Aug 31:

```python
# on the box, from ~/season_archive/WY2025-26/tables/*_current.txt
sel = d[(d.date >= "2025-09-01") & (d.date <= "2026-08-31")]   # 365 rows, one per day
# assert continuity per basin; skip (do not write) any that is irregular
```

Why a plain extract works despite the archived tables being corrupt: `results_table_all` is
incremental and tracks by **file count**, so rows written before the label flipped kept their correct
dates and only later rows got the +365 shift. The jump is visible at one point
(`2026-09-09 → 2027-09-10`), and the water year we need lies entirely before it.

Back up on the viewer host before pushing:

```bash
ssh -i ~/.ssh/swe_dashboard ec2-user@13.49.227.116 \
  'mkdir -p ~/MCASS/data_backup_previous_$(date +%Y%m%d) && cp -p ~/MCASS/data/*_previous.txt $_'
scp -i ~/.ssh/swe_dashboard *_previous.txt ec2-user@13.49.227.116:/home/ec2-user/MCASS/data/
```

`*_climate.txt` was left untouched — different column order, no `FC`, and two years stale. Whether it
is meant to be a static baseline or an annually-refreshed one was never resolved.

### 7. Re-enable the cron

```bash
crontab -l | sed 's|^#\(50 15 .*run_2000.sh\)|\1|' | crontab -
crontab -l | grep run_2000
```

Then watch the next scheduled run end-to-end for a clean `run complete` and a sane `Total runtime`.

---

## The zonal-stats dilution: deliberately left alone

**DECISION (Joel, 2026-09-28): stay consistently wrong. This is the legacy code's last season.**

The fix exists — commit `088b602` on branch `docs/season-rollover`, implemented and verified — but
it is **not** deployed, and step 4b is a no-op. Reasoning below; it is sound, not a shortcut.

### Why staying wrong is safe here

The error is a **fixed per-basin multiplicative constant**: exactly
counted-bbox-cells / polygon-cells, verified across 255 basins with
max |observed − predicted| = 0.0 and correlation 1.000000. It does not vary with the data, the
variable, or the day.

Critically, `zonal_stats.py` builds the Q5/Q50/Q95 climatology bands from the **same**
`swe_mean_values_table.csv` as the current series, so both carry the same constant and it **cancels**.
The viewer's actual job — where this year sits against normal — is therefore exactly right today.
Anomalies, percentiles, year-on-year comparison, trends and peak timing are all unaffected. Only the
absolute mm axis is wrong.

Correcting it now would put a ~2x step between the 2025/26 and 2026/27 series in a product that is
being retired, and then TPS2 would introduce a second change at cutover. **One discontinuity at a
deliberate system change beats two.** TPS2 does not share the bug — `catchments.py` uses
`geometry_mask(..., invert=True)` and nothing in `topopyscale2/` uses `crop=True` — so it is already
correct by construction and the cutover is where the step naturally belongs.

### When this stops being safe

If anyone reads **absolute mm** off the viewer rather than relative position. If that comes up, the
whole record is exactly correctable after the fact without re-running anything: multiply each basin
by its own constant, computable at any time from the basin geometry against the raster grid.

<details>
<summary>Original analysis (kept for the record)</summary>

## Decide before step 5: fix the zonal-stats dilution at the same time?

**The MCASS legacy viewer is live and expected to run through 2026/27, so the tables have to be
right.** They currently are not, in two independent ways:

1. **Dates** — the +1-year shift this rollover fixes.
2. **Values** — `results_table_all.py::extract_mean_values` does
   `mask(raster, [geom], crop=True)` then `np.nanmean(...)`. The rasters have `nodata: None`, so
   rasterio fills cells outside the polygon but inside its bounding box with **0**, and `nanmean`
   averages them in as real data. Every basin mean is diluted by roughly bbox/polygon area.
   Measured over 292 basins: **median 1.965×, mean 2.054× (p10 1.63, p90 2.54, up to 3.95×) too
   low.** Cell-level check on CODE 17165: `nanmean` gave 310.77 mm against a true polygon mean of
   601.60 mm.

The gridded S3 product is **not** affected — it is the raw raster. This is a tables-only defect,
and therefore a viewer-only defect.

**The rollover is the right moment to fix it.** The series restarts from zero here, so correcting
it now introduces no visible step change. Fix it mid-season instead and every basin's published
value roughly doubles overnight, which reads as the model breaking.

One-line fix, pick one:

```python
masked, _ = mask(merged_raster, [geom], crop=True, filled=False)
mean_value = masked.mean()          # masked array: excludes outside-polygon cells
# or
masked, _ = mask(merged_raster, [geom], crop=True, nodata=np.nan)
mean_value = np.nanmean(masked)
```

Do **not** "convert zeros to NaN" — that also discards genuine in-polygon zeros (601.60 true vs
604.27 with zeros dropped).

**DECIDED 2026-09-28 (Joel): fold the fix into the rollover.** Implemented and pushed as commit
`088b602` on branch `docs/season-rollover`; deploy it at step 4b.

Verified against a real raster (`swe_merged_reprojected_2026_278.tif`) and the real 295 basins:

```
raster nodata declared as: None          <- the root cause, confirmed directly
ratio new/old   median 1.950  mean 2.053  p10 1.63  p90 2.59  max 3.95
domain mean     old 22.75 -> new 43.96 mm
new >= old for every basin: True         <- dilution only ever understated
NaN introduced by the fix: 0             <- in-polygon NaN handling preserved
```

That reproduces an earlier, independent measurement over 292 basins on a different date
(median 1.965, mean 2.054, p10 1.63, max 3.95).

**Consequence to communicate**: 2026/27 basin values in the viewer will sit ~2x above the 2025/26
history. That is the old series being wrong, not the new one — but to anyone reading the viewer it
looks like a step change, so it is worth saying so before the season gets going.

*(Superseded 2026-09-28 — this consequence is exactly why the fix was not taken. See above.)*

</details>

## Optional: reclaim the runtime creep

Daily runtime grew to 4h35m by 2026-09-27 at 401 season days, against a ~21:34 downsize — the
2026-09-01 run was killed mid-flight by it, silently. Step 4 resets that count, which is the real
fix. If you also want the climate merge smaller, the old ERA5 dailies can be pruned once archived:

```bash
ls /home/ubuntu/sim/snowmapper_2026/master/inputs/climate/forecast/*_2025*.nc | wc -l
# then archive and remove if you want merge_climate_files3 to stop re-merging last season
```

Not required for the rollover.

## Rollback

Step 2 holds everything that step 4 deletes. To revert: re-disable the cron, restore `tables/`,
untar `spatial_tif.tar.gz` and `fsm_sims.tar.gz`, restore the two configs, re-enable the cron.
