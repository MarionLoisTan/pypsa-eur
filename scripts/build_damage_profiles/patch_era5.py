# Script to patch atlite's era5 dataset to include additional variables
# and helper function to create a new copy

import logging
import atlite
import atlite.datasets.era5 as era5_module
from atlite.datasets.era5 import retrieve_data, _rename_and_clean_coords, sanitize_chunks

import shutil
from pathlib import Path
import xarray as xr

logger = logging.getLogger(__name__)


def _open_with_mixed_grib_fallback(original_open, grib_file, chunks=None, tmpdir=None):
    """
    Drop-in replacement for open_with_grib_conventions that handles GRIB files
    containing mixed dataType ('an' + 'fc'). ERA5 hourly GRIBs for forecast
    variables (e.g. wind gust) split the same variable across both types:
    analysis hours (00/06/12/18 UTC) are 'an', forecast hours are 'fc'.
    Both slices are opened separately and concatenated to give a complete
    hourly time series.
    """
    try:
        return original_open(grib_file, chunks=chunks, tmpdir=tmpdir)
    except Exception as e:
        logger.warning(
            f"open_with_grib_conventions failed ({e}), "
            "retrying with dataType filter"
        )
        datasets = []
        for data_type in ("an", "fc"):
            try:
                ds_type = xr.open_dataset(
                    grib_file,
                    engine="cfgrib",
                    time_dims=["valid_time"],
                    ignore_keys=["edition"],
                    coords_as_attributes=[
                        "surface",
                        "depthBelowLandLayer",
                        "entireAtmosphere",
                        "heightAboveGround",
                        "meanSea",
                    ],
                    filter_by_keys={"dataType": data_type},
                    chunks=sanitize_chunks(chunks),
                )
                if ds_type.data_vars:
                    datasets.append(ds_type)
            except Exception:
                pass
        if not datasets:
            raise RuntimeError(
                f"Could not open {grib_file} with dataType 'an' or 'fc'"
            )
        if len(datasets) == 1:
            return datasets[0]
        return xr.concat(datasets, dim="valid_time").sortby("valid_time")


def get_data_wind_gust(retrieval_params):
    # Wind gust GRIBs have mixed dataType ('an'+'fc'); patch
    # open_with_grib_conventions only for this retrieval, then restore it.
    original_open = era5_module.open_with_grib_conventions
    era5_module.open_with_grib_conventions = (
        lambda grib_file, chunks=None, tmpdir=None:
        _open_with_mixed_grib_fallback(original_open, grib_file, chunks, tmpdir)
    )
    try:
        ds = retrieve_data(
            variable=["10m_wind_gust_since_previous_post_processing"],
            **retrieval_params,
        )
    finally:
        era5_module.open_with_grib_conventions = original_open
    ds = _rename_and_clean_coords(ds)
    ds = ds.rename({"fg10": "wnd_gust10m"})
    return ds


def get_data_lake_mix_temperature(retrieval_params):
    ds = retrieve_data(
        variable=["lake_mix_layer_temperature"],
        **retrieval_params,
    )
    ds = _rename_and_clean_coords(ds)
    ds = ds.rename({"lmlt": "lake_s_temp"})
    return ds


def get_data_lake_total_temperature(retrieval_params):
    ds = retrieve_data(
        variable=["lake_total_layer_temperature"],
        **retrieval_params,
    )
    ds = _rename_and_clean_coords(ds)
    ds = ds.rename({"ltlt": "lake_t_temp"})
    return ds


# Registry: maps feature name -> (function, list of output variables)
CUSTOM_FEATURES = {
    "wind_gust": (get_data_wind_gust, ["wnd_gust10m"]),
    "lake_s_temperature": (get_data_lake_mix_temperature, ["lake_s_temp"]),
    "lake_t_temperature": (get_data_lake_total_temperature, ["lake_t_temp"]),
}


def patch_era5(features=None):
    """
    Patch atlite's era5 module at runtime with custom features.

    Injects custom get_data_* functions and registers them in the features
    dict so they are available via cutout.prepare(). The mixed-dataType GRIB
    fix is scoped inside get_data_wind_gust and does not affect other features.

    Parameters
    ----------
    features : list of str, optional
        List of feature names to patch in. Must be keys in CUSTOM_FEATURES.
        If None, all custom features are patched in.

    Raises
    ------
    ValueError
        If a requested feature is not found in CUSTOM_FEATURES.
    """
    to_patch = features if features is not None else list(CUSTOM_FEATURES.keys())
    for feature in to_patch:
        if feature not in CUSTOM_FEATURES:
            raise ValueError(
                f"Unknown feature '{feature}'. "
                f"Available custom features: {list(CUSTOM_FEATURES.keys())}"
            )
        func, output_vars = CUSTOM_FEATURES[feature]
        setattr(era5_module, f"get_data_{feature}", func)
        era5_module.features[feature] = output_vars

    print(f"Patched era5 with features: {to_patch}")


def open_cutout_dataset(cutout):
    """
    Open an atlite Cutout's NetCDF file as an xarray Dataset with chunks
    aligned to the on-disk chunksize_time, avoiding the performance
    degradation from misaligned Dask chunks.

    Parameters
    ----------
    cutout : atlite.Cutout

    Returns
    -------
    xr.Dataset
    """
    path = Path(cutout.path)
    with xr.open_dataset(path) as ds:
        chunksize_time = ds.attrs.get("chunksize_time", 100)
    return xr.open_dataset(path, chunks={"time": chunksize_time})


def copy_cutout_v2(cutout):
    """
    Create a copy of an existing cutout with '_v2' appended to the filename,
    saved in a 'custom' folder in the same directory as the original.
    If the copy already exists, it will be overwritten.

    Parameters
    ----------
    cutout : atlite.Cutout

    Returns
    -------
    atlite.Cutout pointing to the new copy
    """
    old_path = Path(cutout.path)
    new_path = old_path.parent / "custom" / (old_path.stem + "_v2.nc")
    new_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(old_path, new_path)
    return atlite.Cutout(path=new_path)