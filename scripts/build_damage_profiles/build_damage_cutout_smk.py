"""
Snakemake wrapper: prepare a copy of an atlite cutout with additional ERA5
features needed for damage profile calculations (e.g. lake_s_temp, wnd_gust10m).

The features to prepare are passed via snakemake.params.features.

Inputs
------
cutout : path to the source cutout (.nc)

Outputs
-------
cutout : path for the feature-suffixed copy written to the custom subdirectory
         alongside the source cutouts, e.g. {cutout_dir}/custom/{cutout}_{feature_shortcodes}.nc
         where feature_shortcodes encodes the ERA5 features added (e.g. fg10_lmlt).
"""

import logging
from pathlib import Path

import atlite

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

features = snakemake.params.features

src = Path(snakemake.input.cutout)
dst = Path(snakemake.output.cutout)

cutout = atlite.Cutout(path=src)
derived = cutout.copy(dst)
logger.info(f"Copied {src} → {dst}")

derived.prepare(features=features)
logger.info(f"Prepared features {features} on {dst}")
