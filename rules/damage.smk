# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import yaml
from pathlib import Path

_DMG_CFG = yaml.safe_load(
    open(Path(workflow.basedir) / "config/damage_config.yaml")
)

# Resolve the absolute cutout directory from damage_config.yaml.
# Used as the source path for build_damage_cutout, bypassing the standard
# atlite dataset versioning path so that already-built cutouts in cutout_dir
# are used directly without triggering build_cutout.
_raw = Path(_DMG_CFG["cutout_dir"]).expanduser()
_DAMAGE_CUTOUT_DIR = _raw if _raw.is_absolute() else Path(workflow.basedir) / _raw
# Custom (feature-suffixed) cutouts are written to a 'custom' subfolder alongside the source cutouts.
_DAMAGE_CUTOUT_CUSTOM_DIR = _DAMAGE_CUTOUT_DIR / "custom"

# Resolve which cutouts to prepare:
#   - explicit cutout_names list in damage_config.yaml, OR
#   - all .nc files in cutout_dir (excluding _v2 files)
if _DMG_CFG.get("cutout_names"):
    _DAMAGE_CUTOUT_NAMES = list(_DMG_CFG["cutout_names"])
else:
    _DAMAGE_CUTOUT_NAMES = [
        p.stem for p in sorted(_DAMAGE_CUTOUT_DIR.glob("*.nc")) if "_v2" not in p.stem
    ]

# Build the output filename suffix from ERA5 shortcodes for the selected features.
# e.g. features [wind_gust, lake_s_temperature] → suffix "fg10_lmlt"
_shortcodes = _DMG_CFG.get("feature_shortcodes", {})
_DAMAGE_CUTOUT_SUFFIX = "_".join(
    _shortcodes[f] for f in _DMG_CFG["features"] if f in _shortcodes
)

# Map scenario damage config keys to the resource path pattern for each technology's
# bus-level damage profile. Used by input_damage_profiles() in build_electricity.smk
# and the solve rules to conditionally add damage profile inputs.
_DAMAGE_PROFILE_PATTERNS = {
    "nuclear":    "damage_profiles/nuclear_damage_{clusters}.nc",
    "onwind":     "damage_profiles/onwind_damage_{clusters}.nc",
    "offwind-ac": "damage_profiles/offwind-ac_damage_{clusters}.nc",
    "offwind-dc": "damage_profiles/offwind-dc_damage_{clusters}.nc",
}

# Resolve which runs require nuclear damage profiles.
# When scenarios are enabled, only include runs with damage.nuclear: true.
# When scenarios are disabled, fall back to all run names.
if scenarios:
    # Per-technology list of runs that have damage enabled (any phase).
    # Keyed by technology name matching _DAMAGE_PROFILE_PATTERNS.
    _damage_runs = {
        tech: [
            r for r, cfg in scenarios.items()
            if ((cfg or {}).get("damage") or {}).get(tech, False)
        ]
        for tech in _DAMAGE_PROFILE_PATTERNS
    }
    # Runs where any tech has damage applied at the dispatch phase.
    _dispatch_damage_runs = [
        r for r, cfg in scenarios.items()
        if any(
            v == "dispatch"
            for v in ((cfg or {}).get("damage") or {}).values()
        )
    ]
else:
    _damage_runs = {tech: [] for tech in _DAMAGE_PROFILE_PATTERNS}
    _dispatch_damage_runs = []


rule build_all_damage_cutouts:
    """
    Convenience target: prepare feature-suffixed cutouts for all entries in
    config/damage_config.yaml (cutout_names list, or all .nc files in cutout_dir).

    Usage:
        snakemake build_all_damage_cutouts --configfile config/config.yaml -j4
    """
    input:
        expand(
            str(_DAMAGE_CUTOUT_CUSTOM_DIR / ("{cutout}_" + _DAMAGE_CUTOUT_SUFFIX + ".nc")),
            cutout=_DAMAGE_CUTOUT_NAMES,
        ),


rule build_damage_cutout:
    """
    Produce a copy of an atlite cutout with additional ERA5 features required
    for damage profile calculations (e.g. lake_s_temp, wnd_gust10m).

    Which features are prepared is controlled by the 'features' list in
    config/damage_config.yaml. The output filename encodes the added features
    via their ERA5 shortcodes (e.g. {cutout}_fg10_lmlt.nc).

    The source cutout is read directly from cutout_dir in damage_config.yaml,
    bypassing the standard atlite dataset versioning path so that already-built
    cutouts are used without triggering build_cutout.
    """
    params:
        features=_DMG_CFG["features"],
    input:
        cutout=lambda w: str(_DAMAGE_CUTOUT_DIR / (w.cutout + ".nc")),
    output:
        cutout=str(_DAMAGE_CUTOUT_CUSTOM_DIR / ("{cutout}_" + _DAMAGE_CUTOUT_SUFFIX + ".nc")),
    log:
        "logs/build_damage_cutout_{cutout}.log",
    benchmark:
        "benchmarks/build_damage_cutout_{cutout}"
    threads: config["atlite"].get("nprocesses", 4)
    resources:
        mem_mb=config["atlite"].get("nprocesses", 4) * 1000,
    script:
        "../scripts/build_damage_profiles/build_damage_cutout_smk.py"


rule build_all_nuclear_damage_profiles:
    """
    Convenience target: build bus-level nuclear damage profiles for all
    cluster values defined in config.scenario.clusters.

    Usage:
        snakemake build_all_nuclear_damage_profiles --configfile config/config.yaml -j4
    """
    input:
        expand(
            resources("damage_profiles/nuclear_damage_{clusters}.nc"),
            clusters=config["scenario"]["clusters"],
            run=_damage_runs["nuclear"],
        ),

rule build_nuclear_damage_profile:
    """
    Build hourly nuclear damage profiles aggregated to bus level.

    Uses lake surface temperature from the damage cutout and capacity-weighted
    averaging over plants at each bus. Output is used by
    solve_operations_network_damaged rules.
    """
    params:
        swt=_DMG_CFG["nuclear"]["shutdown_water_temp"],
        dwt=_DMG_CFG["nuclear"]["design_water_temp"],
        sp=_DMG_CFG["nuclear"]["shutdown_period"],
        snapshots=config_provider("snapshots"),
        drop_leap_day=config_provider("enable", "drop_leap_day"),
    input:
        cutout=lambda w: input_cutout(
            w,
            cutout_names=[
                name for name in config_provider("atlite", "cutouts")(w).keys()
                if name not in config.get("atlite", {}).get("cutouts", {})
            ],
        ),
        powerplants=resources("powerplants_s_{clusters}.csv"),
    output:
        profile=resources("damage_profiles/nuclear_damage_{clusters}.nc"),
    log:
        logs("build_nuclear_damage_profile_{clusters}.log"),
    benchmark:
        benchmarks("build_nuclear_damage_profile_{clusters}")
    threads: 1
    resources:
        mem_mb=4000,
    script:
        "../scripts/build_damage_profiles/build_nuclear_damage_profiles.py"


rule build_nuclear_plant_damage_profile:
    """
    Build hourly per-plant nuclear damage profiles for diagnostics.

    Cluster-independent: plant locations and lake temperatures do not vary
    with the network clustering. Uses the first clusters value from
    config.scenario.clusters to locate the powerplants CSV.
    """
    params:
        swt=_DMG_CFG["nuclear"]["shutdown_water_temp"],
        dwt=_DMG_CFG["nuclear"]["design_water_temp"],
        sp=_DMG_CFG["nuclear"]["shutdown_period"],
        snapshots=config_provider("snapshots"),
        drop_leap_day=config_provider("enable", "drop_leap_day"),
    input:
        cutout=lambda w: input_cutout(
            w,
            cutout_names=[
                name for name in config_provider("atlite", "cutouts")(w).keys()
                if name not in config.get("atlite", {}).get("cutouts", {})
            ],
        ),
        powerplants=resources(
            "powerplants_s_" + str(config["scenario"]["clusters"][0]) + ".csv"
        ),
    output:
        plant_profile=resources("damage_profiles/nuclear_damage_plants.nc"),
    log:
        logs("build_nuclear_plant_damage_profile.log"),
    benchmark:
        benchmarks("build_nuclear_plant_damage_profile")
    threads: 1
    resources:
        mem_mb=4000,
    script:
        "../scripts/build_damage_profiles/build_nuclear_damage_profiles.py"

rule solve_all_damaged_dispatch:
    """
    Convenience target: run solve_operations_network_damaged for all scenarios
    with dispatch-phase damage (nuclear: dispatch or nuclear: true).

    Usage:
        snakemake solve_all_damaged_dispatch --configfile config/config.yaml -j4
    """
    input:
        expand(
            RESULTS + "networks/base_s_{clusters}_elec_{opts}_damaged-dispatch.nc",
            run=_dispatch_damage_runs,
            clusters=config["scenario"]["clusters"],
            opts=config["scenario"]["opts"],
        ),


rule solve_all_scenarios:
    """
    Convenience target: solve all scenarios defined in the scenarios file.
    Runs solve_network for every run, plus solve_operations_network_damaged
    for any scenario with dispatch-phase damage.

    Usage:
        snakemake solve_all_scenarios --configfile config/config.yaml -j4
    """
    input:
        expand(
            RESULTS + "networks/base_s_{clusters}_elec_{opts}.nc",
            run=config["run"]["name"],
            clusters=config["scenario"]["clusters"],
            opts=config["scenario"]["opts"],
        ),
        expand(
            RESULTS + "networks/base_s_{clusters}_elec_{opts}_damaged-dispatch.nc",
            run=_dispatch_damage_runs,
            clusters=config["scenario"]["clusters"],
            opts=config["scenario"]["opts"],
        ),