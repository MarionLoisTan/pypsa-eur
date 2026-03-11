import logging
import sys
import yaml
import atlite
from pathlib import Path

# Ensure patch_era5.py (in the same folder) is importable from any working directory
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

from patch_era5 import patch_era5, copy_cutout_v2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

pypsa_eur_root = script_dir.parent.parent

# Load config (damage_config.yaml lives in the same folder as this script)
with open(script_dir / "damage_config.yaml") as f:
    cfg = yaml.safe_load(f)

# Resolve cutout directory:
#   ~/...     → expanded to full home path
#   /abs/path → used as-is
#   relative  → resolved from pypsa-eur root
cutout_dir = Path(cfg["cutout_dir"]).expanduser()
if not cutout_dir.is_absolute():
    cutout_dir = pypsa_eur_root / cutout_dir

# Determine source cutouts to process
# - If cutout_names is defined: use that explicit list
# - Otherwise: all .nc files in cutout_dir (non-recursive), excluding _v2 files
features = cfg["features"]
if cfg.get("cutout_names"):
    cutout_paths = [cutout_dir / (name + ".nc") for name in cfg["cutout_names"]]
else:
    cutout_paths = [p for p in sorted(cutout_dir.glob("*.nc")) if "_v2" not in p.stem]

# Skip cutouts whose _v2 already exists in custom/ (avoid overwriting prepared data)
to_process = []
for cutout_path in cutout_paths:
    v2_path = cutout_path.parent / "custom" / (cutout_path.stem + "_v2.nc")
    if v2_path.exists():
        logger.info(f"Skipping {cutout_path.name} — {v2_path} already exists")
    else:
        to_process.append(cutout_path)

if not to_process:
    logger.info("All cutouts already processed. Nothing to do.")
else:
    # Patch era5 module once for all needed features
    patch_era5(features=features)

    for cutout_path in to_process:
        logger.info(f"Processing {cutout_path.name}")
        cutout = atlite.Cutout(path=cutout_path)
        cutout_v2 = copy_cutout_v2(cutout)
        cutout_v2.prepare(features=features)
        logger.info(f"Done — saved to {cutout_v2.path}")