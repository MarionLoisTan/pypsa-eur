"""
export_water_temps.py

Extract ERA5 lake surface temperatures (full year) per nuclear plant per year
from all discovered scenario cutouts, and save to a long-format CSV.

Usage
-----
    python export_fullyear_temps.py [--config CONFIG_PATH] [--clusters N] [--out OUTPUT_CSV]

Defaults
--------
    --config   config/config.2022-FR+dmg_nucl.yaml  (relative to pypsa-eur root)
    --clusters 5
    --out      fullyear_lake_temps.csv  (written next to this script)
"""

import argparse
import re
import sys
import yaml
import atlite
import pandas as pd
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).resolve().parent
PYPSA_EUR_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PYPSA_EUR_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from scripts.build_damage_profiles.build_nuclear_damage_profiles import load_nuclear_plants


def parse_args():
    p = argparse.ArgumentParser(description="Export fullyear lake temperatures per nuclear plant.")
    p.add_argument("--config",   default="config/config.FR-summer_years+dmg_nucl.yaml",
                   help="Config YAML path relative to pypsa-eur root.")
    p.add_argument("--clusters", type=int, default=5)
    p.add_argument("--ref_year", type=int, default=2022,
                   help="Year whose powerplants_s_<N>.csv is used for all scenarios.")
    p.add_argument("--out",      default=str(SCRIPT_DIR / "fullyear_lake_temps.csv"),
                   help="Output CSV path.")
    return p.parse_args()


def discover_scenarios(cfg, pypsa_eur_root, clusters, ref_year, glob="weather_year_*_dmg_cap"):
    prefix       = cfg["run"].get("prefix", "")
    scenario_dir = pypsa_eur_root / "resources" / prefix
    dirs         = sorted(scenario_dir.glob(glob))
    if not dirs:
        raise FileNotFoundError(f"No scenario directories matched '{glob}' under {scenario_dir}")

    # Load plant definitions once from the reference year
    ref_dirs = [d for d in dirs if f"weather_year_{ref_year}_dmg" in d.name]
    if not ref_dirs:
        raise FileNotFoundError(f"Reference year {ref_year} not found under {scenario_dir}")
    ref_ppl = load_nuclear_plants(ref_dirs[0] / f"powerplants_s_{clusters}.csv")
    print(f"Using powerplants from {ref_dirs[0].name} ({len(ref_ppl)} plants).")

    scenarios = []
    for d in dirs:
        m = re.search(r"weather_year_(\d+)_dmg", d.name)
        if not m:
            continue
        year = int(m.group(1))
        scenarios.append({"year": year, "dir": d, "powerplants_df": ref_ppl})
    return scenarios


def load_cutout_data(scenario, cfg, pypsa_eur_root):
    year         = scenario["year"]
    source       = cfg["data"]["cutout"]["source"]
    version      = cfg["data"]["cutout"]["version"]
    default_name = cfg["atlite"]["default_cutout"]
    cutout_name  = re.sub(r"(?<=-)\d{4}(?=-)", str(year), default_name)
    path         = pypsa_eur_root / "data/cutout" / source / version / (cutout_name + ".nc")
    if not path.exists():
        raise FileNotFoundError(f"Cutout not found: {path}")
    return atlite.Cutout(path=path).data


def extract_lake_temps(scenarios, cfg, pypsa_eur_root):
    records = []
    for s in scenarios:
        year        = s["year"]
        year_start  = f"{year}-01-01"
        year_end    = f"{year}-12-31"
        print(f"  [{year}] loading cutout ...", flush=True)
        cutout_data = load_cutout_data(s, cfg, pypsa_eur_root)

        for _, row in s["powerplants_df"].iterrows():
            plant = row["Name"]
            temps = (
                cutout_data["lake_s_temp"]
                .sel(x=row["lon"], y=row["lat"], method="nearest")
                .to_pandas()
                .loc[year_start:year_end]
            ) - 273.15

            for ts, temp_c in temps.items():
                records.append({
                    "year":        year,
                    "plant":       plant,
                    "snapshot":    ts,
                    "lake_temp_c": round(temp_c, 4),
                })
        print(f"  [{year}] done — {len(s['powerplants_df'])} plants extracted.", flush=True)

    return pd.DataFrame(records)


def main():
    args = parse_args()
    config_path = PYPSA_EUR_ROOT / args.config

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    print("Discovering scenarios ...")
    scenarios = discover_scenarios(cfg, PYPSA_EUR_ROOT, args.clusters, args.ref_year)
    print(f"Found {len(scenarios)} scenario(s): {[s['year'] for s in scenarios]}")

    print("Extracting fullyear temperatures ...")
    df = extract_lake_temps(scenarios, cfg, PYPSA_EUR_ROOT)

    out = Path(args.out)
    df.to_csv(out, index=False)
    print(f"Saved {len(df):,} rows to {out}")


if __name__ == "__main__":
    main()
