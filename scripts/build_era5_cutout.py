"""
Standalone script to build an ERA5 cutout equivalent to europe-2022-era5.nc,
optionally including damage features (wind_gust, lake_s_temperature).

Two modes controlled by ADD_DAMAGE_FEATURES:

  False — build a standard cutout in one step (same as the PyPSA-Eur workflow).

  True  — build the standard cutout, then copy it to a separate path and
          prepare the damage features on the copy. This mirrors the Snakemake
          damage workflow and keeps the base cutout intact.
          Requires the forked atlite (damage-features branch).

Adjust the configuration block below before running.
Requires a valid CDS API key (~/.cdsapirc).

Usage
-----
    pixi run python scripts/build_era5_cutout.py
"""

import atlite

# --- Configuration -----------------------------------------------------------

OUTPUT_PATH = "cutouts/europe-2024-era5.nc"        # base cutout output path

# Set to True to also prepare wind_gust and lake_s_temperature on a copy.
# Requires the forked atlite with the damage-features branch installed.
ADD_DAMAGE_FEATURES = False
DAMAGE_OUTPUT_PATH = "cutouts/custom/europe-2024-era5_fg10_lmlt.nc"

TIME = "2024"   # (start_year, end_year) or a single "YYYY-MM"

X = slice(-12.0, 42.0)   # longitude bounds (west, east)
Y = slice(33.0, 72.0)    # latitude bounds (south, north)
DX = 0.25                 # longitude resolution in degrees
DY = 0.25                 # latitude resolution in degrees

BASE_FEATURES = [
    "height",
    "runoff",
    "influx",
    "temperature",
    "wind",
]

DAMAGE_FEATURES = [
    "wind_gust",
    "lake_s_temperature",
]

# -----------------------------------------------------------------------------

cutout = atlite.Cutout(
    path=OUTPUT_PATH,
    module="era5",
    x=X,
    y=Y,
    dx=DX,
    dy=DY,
    time=TIME,
    chunks={"time": 100},
)

cutout.prepare(
    features=BASE_FEATURES,
    monthly_requests=True,   # requests data month-by-month to stay within CDS size limits
)

if ADD_DAMAGE_FEATURES:
    derived = cutout.copy(DAMAGE_OUTPUT_PATH)
    derived.prepare(features=DAMAGE_FEATURES)
