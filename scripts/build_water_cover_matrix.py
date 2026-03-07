# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT
"""
Compute the fraction of each bus region covered by inland water bodies using
CORINE land cover data.

The script uses ``atlite.ExclusionContainer`` with CORINE raster codes for:
- Water courses (rivers, canals): raster code 35 (CLC class 511)
- Water bodies (lakes, reservoirs): raster code 36 (CLC class 512)

Note: PyPSA-Eur uses sequential CORINE raster values (e.g. 44 = sea and ocean),
not the 3-digit CLC hierarchy codes (e.g. 523 = sea and ocean). The inland water
classes 511 and 512 correspond to raster values 35 and 36 respectively.

Using ``invert=True`` in ``add_raster`` means: keep only cells matching these
codes. The resulting ``availabilitymatrix`` therefore gives, for each cutout
grid cell and each bus region, the fraction of that cell that is inland water.

Inputs
------
- corine: CORINE Land Cover GeoTIFF (sequential raster values)
- regions: Clustered onshore bus regions GeoJSON
- cutout: ERA5/SARAH cutout NetCDF file

Outputs
-------
- ``resources/water_cover_matrix_{clusters}.nc``: xarray.DataArray with
  dimensions ``(bus, y, x)`` and values in [0, 1] representing the fraction
  of each cutout grid cell that is inland water per bus region.
"""

import logging
import time

import atlite
import geopandas as gpd

from scripts._helpers import configure_logging, load_cutout, set_scenario_config

logger = logging.getLogger(__name__)

# CORINE sequential raster codes for inland water:
# 35 = Water courses (CLC 511: rivers, streams, canals)
# 36 = Water bodies  (CLC 512: lakes, reservoirs, ponds)
CORINE_INLAND_WATER_CODES = [35, 36]


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("build_water_cover_matrix", clusters=50)
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    nprocesses = int(snakemake.threads)
    noprogress = snakemake.config["run"].get("disable_progressbar", True)
    noprogress = noprogress or not snakemake.config["atlite"]["show_progress"]

    params = snakemake.params
    res = params.get("excluder_resolution", 100)
    codes = params.get("water_codes", CORINE_INLAND_WATER_CODES)

    cutout = load_cutout(snakemake.input.cutout)

    regions = gpd.read_file(snakemake.input.regions)
    assert not regions.empty, (
        f"List of regions in {snakemake.input.regions} is empty, "
        "cannot compute water cover matrix."
    )
    regions = regions.set_index("name").rename_axis("bus")

    # invert=True: keep ONLY cells with these codes (exclude everything else)
    # → availability values = fraction of each grid cell that IS inland water
    excluder = atlite.ExclusionContainer(crs=3035, res=res)
    excluder.add_raster(snakemake.input.corine, codes=codes, invert=True, crs=3035)

    logger.info(
        f"Computing water cover matrix for {len(regions)} bus regions "
        f"using CORINE codes {codes} at {res}m resolution..."
    )
    start = time.time()

    kwargs = dict(nprocesses=nprocesses, disable_progressbar=noprogress)
    water_cover = cutout.availabilitymatrix(regions, excluder, **kwargs)
    logger.info(f"Completed in {time.time() - start:.2f}s")

    water_cover.name = "water_cover"
    water_cover.attrs.update(
        {
            "description": (
                "Fraction of each cutout grid cell covered by inland water bodies "
                "(CORINE codes 35=water courses, 36=water bodies) per bus region."
            ),
            "corine_codes": str(codes),
            "excluder_resolution_m": res,
            "units": "fraction [0, 1]",
        }
    )

    water_cover.to_netcdf(snakemake.output[0])
