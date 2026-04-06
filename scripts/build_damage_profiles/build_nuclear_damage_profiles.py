"""
Build time-varying damage profiles for active nuclear power plants based on
lake surface temperature from an atlite cutout.

Damage logic (per timestep, per plant):
  - lake_temp <= DWT              : no damage  (profile = 1.0)
  - DWT < lake_temp <= SWT        : partial derating via vulnerability table
                                    profile = 1 - vulnerability(round(lake_temp - DWT))
  - lake_temp > SWT               : full shutdown for current + next SP timesteps
                                    profile = 0.0  (overrides partial derating)

Parameters
----------
SWT : K  — shutdown water temperature threshold (from damage_config.yaml)
DWT : K  — design water temperature (from damage_config.yaml)
SP  : hours — shutdown period (from damage_config.yaml)

Snakemake inputs
----------------
cutout      : path to a prepared cutout containing lake_s_temp
powerplants : path to resources/powerplants_s_{clusters}.csv

Snakemake outputs
-----------------
profile       : resources/damage_profiles/nuclear_damage_{clusters}.nc
                Variable 'profile', dims (time, bus), values in [0, 1].
                Bus-level capacity-weighted average of per-plant profiles.
plant_profile : resources/damage_profiles/nuclear_damage_plants.nc
                Variable 'profile', dims (time, plant), values in [0, 1].
                Per-plant diagnostic output (plant names as coordinates).
"""

import logging
import sys
from pathlib import Path

import atlite
import numpy as np
import pandas as pd
import xarray as xr
import yaml

_SCRIPT_DIR = Path(__file__).parent
_PYPSA_ROOT = _SCRIPT_DIR.parent.parent

logger = logging.getLogger(__name__)


def load_damage_config() -> dict:
    """Load damage_config.yaml from config/."""
    with open(_PYPSA_ROOT / "config" / "damage_config.yaml") as f:
        return yaml.safe_load(f)

# ---------------------------------------------------------------------------
# Vulnerability table
# ---------------------------------------------------------------------------
def _load_vulnerability_table():
    csv_path = _SCRIPT_DIR / "water_temperature_vulnerability.csv"
    df = pd.read_csv(csv_path)
    return dict(zip(df["threshold"].astype(int), df["vulnerability"]))


VULNERABILITY = _load_vulnerability_table()


def get_vulnerability(degrees_above_dwt: float) -> float:
    """Map degrees above DWT to fraction-inoperable vulnerability (0–1)."""
    threshold = min(17, max(0, round(degrees_above_dwt)))
    return VULNERABILITY[threshold]


# ---------------------------------------------------------------------------
# Plant loading
# ---------------------------------------------------------------------------
def load_nuclear_plants(powerplants_csv: Path, dateout_cutoff: int = 2023) -> pd.DataFrame:
    """Return nuclear plants with DateOut > cutoff (still operating)."""
    df = pd.read_csv(powerplants_csv, index_col=0)
    mask = (df["Fueltype"] == "Nuclear") & (df["DateOut"] > dateout_cutoff)
    return df.loc[mask, ["Name", "lat", "lon", "Capacity", "bus"]].copy()


# ---------------------------------------------------------------------------
# Temperature extraction
# ---------------------------------------------------------------------------
def extract_lake_temp(cutout_data, lat: float, lon: float, time_index: pd.DatetimeIndex) -> np.ndarray:
    """
    Extract lake_s_temp time series (Kelvin) at the nearest grid cell to (lat, lon).

    Parameters
    ----------
    cutout_data : xr.Dataset  (cutout.data)
    lat, lon    : plant coordinates
    time_index  : snapshot DatetimeIndex to select
    """
    series = cutout_data["lake_s_temp"].sel(x=lon, y=lat, method="nearest")
    return series.sel(time=time_index).values


# ---------------------------------------------------------------------------
# Damage profile computation
# ---------------------------------------------------------------------------
def compute_damage_profile(
    lake_temp_series: np.ndarray,
    swt: float,
    dwt: float,
    sp: int,
) -> np.ndarray:
    """
    Compute hourly damage profile for a single plant.

    Returns an array of values in [0, 1] where:
      1.0   = fully operable
      0.0   = fully inoperable (shut down)
      (0,1) = partially derated

    Two-pass approach so that full shutdowns always override partial derating.
    """
    n = len(lake_temp_series)
    damage = np.ones(n, dtype=float)

    # Pass 1: partial derating for DWT < T <= SWT
    for t in range(n):
        T = lake_temp_series[t]
        if dwt < T <= swt:
            vuln = get_vulnerability(T - dwt)
            damage[t] = 1.0 - vuln

    # Pass 2: full shutdown for T > SWT (overrides partial derating)
    for t in range(n):
        if lake_temp_series[t] > swt:
            end = min(t + sp + 1, n)
            damage[t:end] = 0.0

    return damage


# ---------------------------------------------------------------------------
# Snakemake entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if "snakemake" not in globals():
        raise RuntimeError(
            "This script is designed to be run via Snakemake. "
            "For the standalone version use build_nuclear_damage_profiles_old.py."
        )

    configure_logging = None
    try:
        sys.path.insert(0, str(_SCRIPT_DIR.parent))
        from _helpers import configure_logging
        configure_logging(snakemake)
    except Exception:
        logging.basicConfig(level=logging.INFO)

    swt = snakemake.params.swt
    dwt = snakemake.params.dwt
    sp = snakemake.params.sp

    snap_cfg = snakemake.params.snapshots
    drop_leap = snakemake.params.drop_leap_day

    snapshot_index = pd.date_range(
        start=snap_cfg["start"],
        end=snap_cfg["end"],
        freq="h",
        inclusive=snap_cfg.get("inclusive", "left"),
    )
    if drop_leap:
        snapshot_index = snapshot_index[
            ~((snapshot_index.month == 2) & (snapshot_index.day == 29))
        ]

    powerplants_csv = Path(snakemake.input.powerplants)
    nuclear = load_nuclear_plants(powerplants_csv)
    logger.info(f"Found {len(nuclear)} active nuclear plants.")

    want_bus = hasattr(snakemake.output, "profile")
    want_plant = hasattr(snakemake.output, "plant_profile")

    # Resolve where to save the per-plant profile:
    #   - declared output when running build_nuclear_plant_damage_profile
    #   - sibling file next to the bus profile when running build_nuclear_damage_profile
    if want_plant:
        plant_profile_path = Path(snakemake.output.plant_profile)
    else:
        plant_profile_path = (
            Path(snakemake.output.profile).parent / "nuclear_damage_plants.nc"
        )

    cutout = atlite.Cutout(path=snakemake.input.cutout)
    cutout_data = cutout.data

    # --- Step 1: per-plant profiles (always computed and saved) ---
    plant_profiles = {}
    for _, plant in nuclear.iterrows():
        lake_temp = extract_lake_temp(
            cutout_data, plant["lat"], plant["lon"], snapshot_index
        )
        plant_profiles[plant["Name"]] = compute_damage_profile(
            lake_temp, swt=swt, dwt=dwt, sp=sp
        )

    plant_df = pd.DataFrame(plant_profiles, index=snapshot_index)

    plant_profile_path.parent.mkdir(parents=True, exist_ok=True)
    plant_da = xr.DataArray(
        plant_df.values,
        dims=["time", "plant"],
        coords={"time": snapshot_index, "plant": plant_df.columns.tolist()},
    )
    xr.Dataset({"profile": plant_da}).to_netcdf(plant_profile_path)
    logger.info(f"Saved per-plant profile: {plant_profile_path}")

    # --- Step 2: bus-level aggregation (capacity-weighted mean of per-plant profiles) ---
    if want_bus:
        bus_profiles = {}
        for bus in nuclear["bus"].unique():
            plants_at_bus = nuclear[nuclear["bus"] == bus].set_index("Name")
            common = plants_at_bus.index.intersection(plant_df.columns)
            weights = plants_at_bus.loc[common, "Capacity"]
            bus_profiles[bus] = plant_df[common].dot(weights) / weights.sum()

        bus_df = pd.DataFrame(bus_profiles, index=snapshot_index)

        bus_da = xr.DataArray(
            bus_df.values,
            dims=["time", "bus"],
            coords={"time": snapshot_index, "bus": bus_df.columns.tolist()},
        )
        xr.Dataset({"profile": bus_da}).to_netcdf(snakemake.output.profile)
        logger.info(f"Saved bus-level profile: {snakemake.output.profile}")
