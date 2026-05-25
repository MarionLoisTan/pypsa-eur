"""
Build time-varying damage profiles for active nuclear power plants based on
lake surface temperature from an atlite cutout.

Damage logic (per timestep, per plant):
  - lake_temp <= DWT              : no damage  (profile = 0.0)
  - DWT < lake_temp <= SWT        : damage = max(vulnerability, regulation)
      vulnerability: profile = interp((lake_temp - DWT) * C, vulnerability_table)
      regulation:    profile = interp(SWT - lake_temp, regulation_table)
                     (0.0 at 5°C below SWT, rising steeply to 1.0 at SWT)
  - lake_temp > SWT               : full shutdown for current + next SP timesteps
                                    profile = 1.0  (overrides both mechanisms)

  When binary_shutdown=True (legacy mode), the regulation table is not applied and
  the model reverts to a hard 0/1 step at SWT.

Vulnerability table compression (parameter C):
  The vulnerability lookup uses an effective threshold:
      thresh_eff = (lake_temp - DWT) * C
  This is DWT-anchored: zero damage at DWT is preserved regardless of C.
  C = 1.0  → original table (thresh_eff = degrees above DWT)
  C > 1.0  → same vulnerability reached at lower lake_temp (more aggressive derating)
  C = (17 / (SWT - DWT)) → full vulnerability range compressed into DWT→SWT interval

Regulatory discharge limit (water_temperature_regulations.csv):
  Maps degrees below SWT to fraction inoperable due to regulatory discharge constraints.
  Applies in the 5°C window below SWT; reaches 1.0 at SWT, 0.0 at 5°C below.
  Disabled when binary_shutdown=True.

Parameters
----------
SWT            : K     — shutdown water temperature threshold (from damage_config.yaml)
DWT            : K     — design water temperature (from damage_config.yaml)
SP             : hours — shutdown period (from damage_config.yaml)
C              : float — vulnerability compression factor (from damage_config.yaml)
binary_shutdown: bool  — if True, use legacy hard step at SWT; if False, use regulation ramp

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


def _load_regulation_table():
    csv_path = _SCRIPT_DIR / "water_temperature_regulations.csv"
    df = pd.read_csv(csv_path)
    return dict(zip(df["threshold"].astype(int), df["regulation"]))


REGULATION = _load_regulation_table()


def get_vulnerability(degrees_above_dwt: float, c: float = 1.0) -> float:
    threshold = min(17.0, max(0.0, degrees_above_dwt * c))
    return float(np.interp(threshold, list(VULNERABILITY.keys()), list(VULNERABILITY.values())))


def get_regulation(degrees_below_swt: float) -> float:
    """Map degrees below SWT to fraction inoperable (0-1) from regulatory discharge limit table.
    Clamps to [0, 5]; returns 1.0 at or above SWT, 0.0 at 5+ degrees below SWT.
    """
    threshold = min(5.0, max(0.0, degrees_below_swt))
    return float(np.interp(threshold, list(REGULATION.keys()), list(REGULATION.values())))


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
    c: float = 1.0,
    binary_shutdown: bool = True,
) -> np.ndarray:
    """
    Compute hourly damage profile for a single plant.

    Returns an array of values in [0, 1] where:
      0.0   = no damage (fully operable)
      1.0   = fully inoperable (shut down)
      (0,1) = partially derated

    Two-pass approach so that full shutdowns always override partial derating.
    When binary_shutdown=False, applies regulatory discharge derating in the 5°C
    window below SWT and takes the max with vulnerability derating.
    """
    n = len(lake_temp_series)
    damage = np.zeros(n, dtype=float)

    # Pass 1: derating for T > DWT
    for t in range(n):
        T = lake_temp_series[t]
        if T > dwt:
            vuln = get_vulnerability(T - dwt, c=c)
            if binary_shutdown:
                damage[t] = vuln
            else:
                reg = get_regulation(swt - T)
                damage[t] = max(vuln, reg)

    # Pass 2: SP persistence — full shutdown for sp hours after any T > SWT
    for t in range(n):
        if lake_temp_series[t] > swt:
            end = min(t + sp + 1, n)
            damage[t:end] = 1.0

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

    swt             = snakemake.params.swt
    dwt             = snakemake.params.dwt
    sp              = snakemake.params.sp
    c               = snakemake.params.c
    binary_shutdown = snakemake.params.binary_shutdown

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

    cutout_paths = (
        snakemake.input.cutout
        if isinstance(snakemake.input.cutout, list)
        else [snakemake.input.cutout]
    )
    if len(cutout_paths) == 1:
        cutout_data = atlite.Cutout(path=cutout_paths[0]).data
    else:
        cutout_data = xr.concat(
            [atlite.Cutout(path=p).data for p in cutout_paths],
            dim="time",
        )

    # --- Step 1: per-plant profiles (always computed and saved) ---
    plant_profiles = {}
    for _, plant in nuclear.iterrows():
        lake_temp = extract_lake_temp(
            cutout_data, plant["lat"], plant["lon"], snapshot_index
        )
        plant_profiles[plant["Name"]] = compute_damage_profile(
            lake_temp, swt=swt, dwt=dwt, sp=sp, c=c, binary_shutdown=binary_shutdown
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
