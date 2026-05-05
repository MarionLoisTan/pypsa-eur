"""
Build time-varying damage profiles for wind generators (onwind, offwind-ac,
offwind-dc) based on 10 m wind gust speed from an atlite cutout.

Damage logic (eq. 2.3.1, Ren. Energy 2012, doi:10.1016/j.renene.2012.01.012):
  V_e = V_p * 1.2            (extreme wind speed from 10 m gust V_p)
  F_D = 0                    if V_e <= 25 m/s
  F_D = 0.0002*V_e^2
        - 0.0031*V_e
        - 0.0494             if 25 m/s < V_e < 75 m/s
  F_D = 1                    if V_e >= 75 m/s

F_D is a damage fraction (0 = undamaged, 1 = fully damaged). The output
profile saved to NetCDF is the operational fraction: 1 - F_D, matching
the format used by the nuclear damage profiles and _apply.py.

Snakemake wildcards
-------------------
carrier  : technology name, e.g. "onwind", "offwind-ac", "offwind-dc"
clusters : network clustering level, e.g. "5"

Snakemake inputs
----------------
cutout       : path to a prepared cutout containing wnd_gust10m
availability : path to resources/availability_matrix_{clusters}_{carrier}.nc

Snakemake outputs
-----------------
profile : resources/damage_profiles/{carrier}_damage_{clusters}.nc
          Variable 'profile', dims (time, bus), values in [0, 1].
          Bus-level layout-weighted mean operational fraction.
"""

import logging
import sys
from pathlib import Path

import atlite
import pandas as pd
import xarray as xr

_SCRIPT_DIR = Path(__file__).parent
_PYPSA_ROOT = _SCRIPT_DIR.parent.parent

logger = logging.getLogger(__name__)


def compute_wind_damage(V_p: xr.DataArray) -> xr.DataArray:
    """
    Compute wind damage fraction F_D from 10 m wind gust speed V_p (m/s).

    Converts gust at 10 m height to 90 m height, V_e = V_p * 1.2, then applies
    equation 2.3.1. Works on any shape xarray DataArray.

    Returns
    -------
    F_D : xr.DataArray
        Damage fraction in [0, 1] with the same shape and coordinates as V_p.
        0 = no damage, 1 = fully damaged.
    """
    V_e = V_p * 1.2
    F_D = xr.where(
        V_e <= 25,
        0.0,
        xr.where(
            V_e >= 75,
            1.0,
            0.0002 * V_e ** 2 - 0.0031 * V_e - 0.0494,
        ),
    )
    return F_D


def aggregate_to_buses(
    F_D_grid: xr.DataArray,
    CF_mean: xr.DataArray,
    area: xr.DataArray,
    availability: xr.DataArray,
) -> xr.DataArray:
    """
    Layout-weighted mean F_D per bus, consistent with build_renewable_profiles.py.

    Weights per cell = CF_mean × area × availability.
    capacity_per_sqkm is a scalar and cancels in the weighted ratio.

    Parameters
    ----------
    F_D_grid     : DataArray, dims (time, y, x) — per-cell damage fraction
    CF_mean      : DataArray, dims (y, x) — time-mean capacity factor per cell
    area         : DataArray, dims (y, x) — cell area in km²
    availability : DataArray, dims (bus, y, x)  — per-bus spatial weights

    Returns
    -------
    DataArray, dims (time, bus), values in [0, 1].
    """
    weights = CF_mean * area * availability  # (bus, y, x)
    weighted_sum = (F_D_grid * weights).sum(dim=["y", "x"])
    total_weight = weights.sum(dim=["y", "x"])
    return weighted_sum / total_weight


# ---------------------------------------------------------------------------
# Snakemake entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if "snakemake" not in globals():
        raise RuntimeError(
            "This script is designed to be run via Snakemake."
        )

    try:
        sys.path.insert(0, str(_SCRIPT_DIR.parent))
        from _helpers import configure_logging
        configure_logging(snakemake)
    except Exception:
        logging.basicConfig(level=logging.INFO)

    carrier = snakemake.wildcards.carrier
    clusters = snakemake.wildcards.clusters
    turbine = snakemake.params.turbine

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

    logger.info(f"Building wind damage profile for carrier={carrier}, clusters={clusters}")

    cutout_paths = (
        snakemake.input.cutout
        if isinstance(snakemake.input.cutout, list)
        else [snakemake.input.cutout]
    )
    # Use the first cutout for layout weights (CF_mean and area are time-invariant)
    layout_cutout = atlite.Cutout(path=cutout_paths[0])
    CF_mean = layout_cutout.wind(turbine=turbine, capacity_factor=True)  # (y, x)
    area = layout_cutout.area(crs=3035) / 1e6  # km² per cell, (y, x)

    if len(cutout_paths) == 1:
        V_p = layout_cutout.data["wnd_gust10m"].sel(time=snapshot_index)
    else:
        combined = xr.concat(
            [atlite.Cutout(path=p).data for p in cutout_paths], dim="time"
        )
        V_p = combined["wnd_gust10m"].sel(time=snapshot_index)

    availability = xr.open_dataarray(snakemake.input.availability)

    F_D_grid = compute_wind_damage(V_p)
    logger.info("Computed F_D grid, aggregating to bus level...")

    F_D_bus = aggregate_to_buses(F_D_grid, CF_mean, area, availability)
    operational_profile = 1.0 - F_D_bus

    out_path = Path(snakemake.output.profile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    profile_da = xr.DataArray(
        operational_profile.values,
        dims=["time", "bus"],
        coords={"time": snapshot_index, "bus": availability.coords["bus"].values},
    )
    xr.Dataset({"profile": profile_da}).to_netcdf(out_path)
    logger.info(f"Saved operational profile: {out_path}")
