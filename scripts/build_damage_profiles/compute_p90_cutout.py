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

import atlite
import pandas as pd


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
    cutout = atlite.Cutout(path=cutout_path)
    data = cutout.data[variable]  # DataArray (time, y, x)

    p90 = data.quantile(0.9, dim="time").drop_vars("quantile")

    # Derive year string from the time coordinate
    years = pd.DatetimeIndex(cutout.data.time.values).year.unique()
    year = str(years[0]) if len(years) == 1 else f"{years.min()}-{years.max()}"

    out_name = f"p90-{variable}-{year}-era5.nc"
    out_dir = Path(output_dir) if output_dir is not None else cutout_path.parent
    out_path = out_dir / out_name

    p90.to_netcdf(out_path)
    print(f"Saved: {out_path}")
    return out_path


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
