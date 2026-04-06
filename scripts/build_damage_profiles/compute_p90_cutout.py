"""
Compute the 90th-percentile (p90) value at each grid cell for a given variable
from an atlite cutout.

The p90 is computed over the full time dimension of the cutout, producing a
static (y, x) map of the threshold that is exceeded only 10 % of the time.

Output
------
  p90-{variable}-{year}-era5.nc
  - dimensions : (y, x)
  - coordinates: x (longitude), y (latitude)
  - saved next to the input cutout unless --output-dir is specified
"""

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr


def compute_p90_profile(
    cutout_path: str | Path,
    variable: str,
    output_dir: str | Path | None = None,
) -> Path:
    """
    Compute the per-grid-cell p90 of *variable* over the cutout time dimension.

    Parameters
    ----------
    cutout_path : path to the atlite cutout (.nc file)
    variable    : name of the variable in cutout.data (e.g. 'temperature', 'lake_s_temp')
    output_dir  : directory for the output file; defaults to the cutout's directory

    Returns
    -------
    Path to the written NetCDF file.
    """
    cutout_path = Path(cutout_path).resolve()
    if not cutout_path.exists():
        raise FileNotFoundError(f"Cutout not found: {cutout_path}")
    ds = xr.open_dataset(cutout_path)
    data = ds[variable]  # DataArray (time, y, x)

    p90 = data.quantile(0.9, dim="time").drop_vars("quantile")

    # Derive year string from the time coordinate
    years = pd.DatetimeIndex(ds.time.values).year.unique()
    year = str(years[0]) if len(years) == 1 else f"{years.min()}-{years.max()}"

    out_name = f"p90-{variable}-{year}-era5.nc"
    out_dir = Path(output_dir) if output_dir is not None else cutout_path.parent
    out_path = out_dir / out_name

    p90.to_netcdf(out_path)
    print(f"Saved: {out_path}")
    return out_path


def plot_p90_heatmap(
    p90_path: str | Path,
    regions_path: str | Path | None = None,
    save_path: str | Path | None = None,
    cmap: str = "viridis",
) -> plt.Figure:
    """
    Plot a heatmap of the per-grid-cell p90 values from a p90-*.nc file,
    with optional bus region boundaries overlaid.

    Parameters
    ----------
    p90_path     : path to the p90 NetCDF file produced by compute_p90_profile
    regions_path : optional path to a GeoJSON of bus regions to overlay
                   (e.g. resources/2022-FR/2022-FR-base/regions_onshore_base_s_5.geojson)
    save_path    : optional path to save the figure (e.g. 'p90_map.png');
                   if None the figure is displayed interactively
    cmap         : matplotlib colormap name (default 'viridis')

    Returns
    -------
    matplotlib Figure
    """
    p90_path = Path(p90_path)
    ds = xr.open_dataset(p90_path)

    var_name = list(ds.data_vars)[0]
    da = ds[var_name]

    fig, ax = plt.subplots(figsize=(10, 6))
    da.plot(ax=ax, x="x", y="y", cmap=cmap)

    if regions_path is not None:
        regions = gpd.read_file(regions_path)
        regions.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=0.8)

    ax.set_title(f"p90 — {var_name}  ({p90_path.stem})")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.tight_layout()

    if save_path is not None:
        fig.savefig(save_path, dpi=150)
        print(f"Saved figure: {save_path}")
    else:
        plt.show()

    return fig


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Compute per-grid-cell p90 of a variable from an atlite cutout."
    )
    parser.add_argument("cutout_path", help="Path to the atlite cutout (.nc file)")
    parser.add_argument("variable", help="Variable name in the cutout (e.g. temperature)")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for the output file (default: same directory as cutout)",
    )
    args = parser.parse_args()

    compute_p90_profile(
        cutout_path=args.cutout_path,
        variable=args.variable,
        output_dir=args.output_dir,
    )
