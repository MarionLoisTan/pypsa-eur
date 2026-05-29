# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""
Shared utility for applying technology damage profiles to a PyPSA network.

Used by both prepare_network.py (capacity planning) and
solve_operations_network.py (dispatch re-optimisation).
"""

import logging

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


_VALID_PHASES = frozenset({"capacity", "dispatch"})


def apply_damage_profiles(n, damage_config, snakemake_input, phase):
    """
    Apply time-varying damage profiles to n.generators_t.p_max_pu.

    For each technology with damage enabled in damage_config:
      - Loads the profile from snakemake_input.damage_{tech}
      - Resamples to match the network's snapshot resolution if needed
      - For conventional generators (nuclear): broadcasts the static p_max_pu
        scalar then multiplies by (1 - damage fraction)
      - For renewable generators (onwind, offwind-*): multiplies the existing
        time-varying profile by (1 - damage fraction)

    Parameters
    ----------
    n : pypsa.Network
    damage_config : dict
        e.g. {"nuclear": "capacity", "onwind": "dispatch"}.
        Values: "capacity" (apply only in prepare_network),
                "dispatch" (apply only in solve_operations_network).
        Use false or null in YAML to disable a technology without removing the key.
    snakemake_input : snakemake.input  object with damage_{tech} attributes
    phase : str
        Either "capacity" or "dispatch" — identifies the calling script's phase.
    """
    for tech, enabled in (damage_config or {}).items():
        if enabled is False or enabled is None:
            continue
        if enabled not in _VALID_PHASES:
            raise ValueError(
                f"damage.{tech}: invalid value {enabled!r} "
                f"({type(enabled).__name__}). "
                f"Must be 'capacity' or 'dispatch'. "
                f"Use false or null to disable."
            )
        if enabled != phase:
            continue

        input_key = f"damage_{tech}"
        if not hasattr(snakemake_input, input_key):
            logger.warning(
                f"damage.{tech}=true but Snakemake input '{input_key}' was not provided."
            )
            continue

        profile = (
            xr.open_dataset(getattr(snakemake_input, input_key))["profile"]
            .to_pandas()
        )

        # Align timestamps to network snapshots, resampling if needed
        snapshots = n.snapshots
        if len(snapshots) < len(profile):
            freq = (
                snapshots[1] - snapshots[0]
                if len(snapshots) > 1
                else pd.Timedelta("1h")
            )
            profile = profile.resample(freq).mean().iloc[: len(snapshots)]
        profile.index = snapshots[: len(profile)]

        gens = n.generators[n.generators.carrier == tech]
        if len(gens) == 0:
            logger.warning(f"damage.{tech}=true but no '{tech}' generators found in network.")
            continue

        # Get current p_max_pu per generator:
        #   - time-varying column exists → renewable (subtract from existing series)
        #   - only static scalar exists  → conventional, e.g. nuclear (broadcast then subtract)
        if gens.index[0] in n.generators_t.p_max_pu.columns:
            current = n.generators_t.p_max_pu[gens.index]
        else:
            static = n.generators.loc[gens.index, "p_max_pu"]
            current = pd.DataFrame(
                {g: static[g] for g in gens.index}, index=profile.index
            )

        # Map bus-level damage profile → per-generator (handles shared buses).
        # Profile values are damage fractions [0, 1] where 0 = no damage, 1 = fully damaged.
        # Invert to availability fraction and multiply against current p_max_pu.
        gen_damage = pd.DataFrame(
            {gen: profile[bus] for gen, bus in gens["bus"].items()},
            index=profile.index,
        )

        n.generators_t.p_max_pu[gens.index] = current * (1 - gen_damage)
        logger.info(f"Applied {tech} damage profile to {len(gens)} generators.")


def add_load_shedding(n):
    """Add load shedding generators — detects electricity-only vs sector-coupled."""
    has_heat = any(n.buses.carrier.str.contains("heat", na=False))

    if has_heat:
        nodes_LV = n.buses.query('carrier == "low voltage"').index
        nodes_heat1 = n.buses.query('carrier == "rural heat"').index
        nodes_heat2 = n.buses.query('carrier == "urban central heat"').index
        nodes_heat3 = n.buses.query('carrier == "urban decentral heat"').index

        n.add("Carrier", "load_el")
        n.add("Carrier", "load_heat")

        for bus in nodes_LV:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier="load_el",
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)

        for bus in nodes_heat1:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier="load_heat",
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)

        for bus in nodes_heat2:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier="load_heat",
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)

        for bus in nodes_heat3:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier="load_heat",
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
    else:
        n.add("Carrier", "load_shedding")
        for bus in n.buses.index:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier="load_shedding",
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)

    return n
