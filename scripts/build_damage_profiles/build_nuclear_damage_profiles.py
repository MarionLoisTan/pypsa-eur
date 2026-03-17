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
SWT : 305 K  (32 °C + 273)
DWT : 293 K  (20 °C + 273)
SP  : 24 hours (shutdown propagation window)

Inputs
------
config_path : path to config.yaml (main PyPSA-Eur config)

The cutout path, powerplants CSV path, snapshot period, and output directory
are all derived from the config and the scenarios file referenced within it.

Output
------
One CSV per scenario where damage.nuclear is true, saved to:
  resources/{RDIR}/nuclear_damage_profiles/nuclear_damage.csv
  - index   : hourly timestamps (snapshot period)
  - columns : one per nuclear plant (plant Name)
"""

from pathlib import Path

import atlite
import numpy as np
import pandas as pd
import yaml

_SCRIPT_DIR = Path(__file__).parent
_PYPSA_ROOT = _SCRIPT_DIR.parent.parent

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
# Damage config helpers
# ---------------------------------------------------------------------------
def load_damage_config() -> dict:
    """Load damage_config.yaml from the same directory as this script."""
    with open(_SCRIPT_DIR / "damage_config.yaml") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def load_configs(config_path: str | Path):
    """Return (main_config dict, scenarios dict)."""
    config_path = Path(config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)

    scenarios_file = Path(config["run"]["scenarios"]["file"])
    if not scenarios_file.is_absolute():
        scenarios_file = _PYPSA_ROOT / scenarios_file

    with open(scenarios_file) as f:
        scenarios = yaml.safe_load(f)

    return config, scenarios


def _get_rdir(config: dict, scenario_name: str) -> str:
    """Reconstruct RDIR string following the same logic as _helpers.get_rdir()."""
    run = config["run"]
    prefix = run.get("prefix", "")
    scenarios_enabled = run.get("scenarios", {}).get("enable", False)

    if run.get("name") and scenarios_enabled:
        rdir = f"{scenario_name}/"
    elif run.get("name"):
        rdir = f"{run['name']}/"
    else:
        rdir = ""

    if prefix:
        rdir = f"{prefix}/{rdir}"

    return rdir


def get_cutout_path(config: dict) -> Path:
    """
    Derive the cutout path from the config.

    If config has a data.cutout section (source + version), the path is:
      data/cutout/{source}/{version}/{name}.nc
    Otherwise falls back to:
      cutouts/{name}.nc
    """
    name = config["atlite"]["default_cutout"]
    data_cutout = config.get("data", {}).get("cutout", {})
    source = data_cutout.get("source", "")
    version = data_cutout.get("version", "")
    if source and version:
        return _PYPSA_ROOT / "data" / "cutout" / source / version / f"{name}.nc"
    return _PYPSA_ROOT / "cutouts" / f"{name}.nc"


def get_powerplants_path(config: dict, scenario_name: str) -> Path:
    """Return resources/{RDIR}/powerplants_s_{clusters}.csv."""
    rdir = _get_rdir(config, scenario_name)
    clusters = config["scenario"]["clusters"][0]
    return _PYPSA_ROOT / "resources" / rdir / f"powerplants_s_{clusters}.csv"


def get_output_path(config: dict, scenario_name: str) -> Path:
    """Return resources/{RDIR}/nuclear_damage.csv."""
    rdir = _get_rdir(config, scenario_name)
    return _PYPSA_ROOT / "resources" / rdir / "nuclear_damage.csv"


def get_bus_profile_output_path(config: dict, scenario_name: str) -> Path:
    """Return resources/{RDIR}/profile_{clusters}_nuclear.csv."""
    rdir = _get_rdir(config, scenario_name)
    clusters = config["scenario"]["clusters"][0]
    return _PYPSA_ROOT / "resources" / rdir / f"profile_{clusters}_nuclear.csv"


def get_snapshot_index(config: dict, scenario_cfg: dict) -> pd.DatetimeIndex:
    """Return hourly DatetimeIndex for the scenario (falls back to main config)."""
    snap = scenario_cfg.get("snapshots", config["snapshots"])
    return pd.date_range(
        start=snap["start"],
        end=snap["end"],
        freq="h",
        inclusive=snap.get("inclusive", "left"),
    )


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
# Main entry point
# ---------------------------------------------------------------------------
def build_nuclear_damage_profiles(config_path: str | Path):
    """
    Build nuclear damage profiles for all scenarios with damage.nuclear == True.

    Cutout path, powerplants CSV, snapshot period, and output paths are all
    derived from the config file.

    Parameters
    ----------
    config_path : path to config.yaml
    """
    config, scenarios = load_configs(config_path)

    dmg_cfg = load_damage_config()["nuclear"]
    swt = dmg_cfg["SWT"]
    dwt = dmg_cfg["DWT"]
    sp = dmg_cfg["SP"]

    # Filter to scenarios that request nuclear damage profiles
    damage_scenarios = {
        name: scen_cfg
        for name, scen_cfg in scenarios.items()
        if scen_cfg and scen_cfg.get("damage", {}).get("nuclear", False)
    }

    if not damage_scenarios:
        print("No scenarios with damage.nuclear == true found. Nothing to do.")
        return {}

    cutout_path = get_cutout_path(config)
    if not cutout_path.exists():
        raise FileNotFoundError(
            f"Cutout not found at {cutout_path}. "
            "Ensure atlite.default_cutout in config.yaml points to a prepared cutout "
            "that contains the lake_s_temp variable."
        )

    # Open cutout once (shared across scenarios)
    cutout = atlite.Cutout(path=cutout_path)
    cutout_data = cutout.data

    results = {}

    for scenario_name, scen_cfg in damage_scenarios.items():
        print(f"\nProcessing scenario: {scenario_name}")

        snapshot_index = get_snapshot_index(config, scen_cfg)
        powerplants_csv = get_powerplants_path(config, scenario_name)

        if not powerplants_csv.exists():
            print(f"  WARNING: powerplants CSV not found at {powerplants_csv}, skipping.")
            continue

        nuclear = load_nuclear_plants(powerplants_csv)
        print(f"  Found {len(nuclear)} active nuclear plants.")

        profiles = {}
        for _, plant in nuclear.iterrows():
            lake_temp = extract_lake_temp(cutout_data, plant["lat"], plant["lon"], snapshot_index)
            profiles[plant["Name"]] = compute_damage_profile(lake_temp, swt=swt, dwt=dwt, sp=sp)

        df_out = pd.DataFrame(profiles, index=snapshot_index)

        out_path = get_output_path(config, scenario_name)
        df_out.to_csv(out_path)
        print(f"  Saved: {out_path}")

        results[scenario_name] = df_out

    return results


# ---------------------------------------------------------------------------
# Bus-level aggregation
# ---------------------------------------------------------------------------
def build_bus_damage_profiles(results: dict, config: dict, scenarios: dict) -> dict:
    """
    Aggregate per-plant damage profiles to bus level using capacity-weighted averaging.

    For each bus, the profile is the capacity-weighted mean of all nuclear plants
    connected to that bus.

    Parameters
    ----------
    results   : {scenario_name: damage_df} as returned by build_nuclear_damage_profiles
    config    : main config dict (from load_configs)
    scenarios : scenarios dict (from load_configs)

    Output
    ------
    One CSV per scenario, saved to:
      resources/{RDIR}/profile_{clusters}_nuclear.csv
      - index   : hourly timestamps
      - columns : one per bus that has at least one nuclear plant
    """
    bus_results = {}

    for scenario_name, damage_df in results.items():
        powerplants_csv = get_powerplants_path(config, scenario_name)
        if not powerplants_csv.exists():
            print(f"  WARNING: powerplants CSV not found at {powerplants_csv}, skipping.")
            continue

        nuclear = load_nuclear_plants(powerplants_csv)

        bus_profiles = {}
        for bus in nuclear["bus"].unique():
            plants_at_bus = nuclear[nuclear["bus"] == bus].set_index("Name")
            common = plants_at_bus.index.intersection(damage_df.columns)
            weights = plants_at_bus.loc[common, "Capacity"]
            bus_profiles[bus] = damage_df[common].dot(weights) / weights.sum()

        df_out = pd.DataFrame(bus_profiles, index=damage_df.index)

        out_path = get_bus_profile_output_path(config, scenario_name)
        df_out.to_csv(out_path)
        print(f"  Saved bus profiles: {out_path}")

        bus_results[scenario_name] = df_out

    return bus_results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build nuclear damage profiles.")
    parser.add_argument("config_path", help="Path to config.yaml")
    args = parser.parse_args()

    config, scenarios = load_configs(args.config_path)
    results = build_nuclear_damage_profiles(config_path=args.config_path)
    build_bus_damage_profiles(results, config, scenarios)