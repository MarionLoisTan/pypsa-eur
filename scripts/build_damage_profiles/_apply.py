# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""
Shared utility for applying technology damage profiles to a PyPSA network.

Used by both prepare_network.py (capacity planning) and
solve_operations_network_damaged.py (dispatch re-optimisation).
"""

import logging

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


def apply_damage_profiles(n, damage_config, snakemake_input, phase):
    """
    Apply time-varying damage profiles to n.generators_t.p_max_pu.

    For each technology with damage enabled in damage_config:
      - Loads the profile from snakemake_input.damage_{tech}
      - Resamples to match the network's snapshot resolution if needed
      - For conventional generators (nuclear): multiplies the static p_max_pu
        scalar by the damage factor so the country-level availability is preserved
      - For renewable generators (onwind, offwind-*): multiplies the existing
        time-varying profile by the damage factor

    Parameters
    ----------
    n : pypsa.Network
    damage_config : dict
        e.g. {"nuclear": "capacity", "onwind": "dispatch"}.
        Values: "capacity" (apply only in prepare_network),
                "dispatch" (apply only in solve_operations_network_damaged),
                True (apply in both phases, legacy/backward compat).
    snakemake_input : snakemake.input  object with damage_{tech} attributes
    phase : str
        Either "capacity" or "dispatch" — identifies the calling script's phase.
    """
    for tech, enabled in (damage_config or {}).items():
        if not enabled:
            continue
        # String phase tag: skip if this call is for a different phase.
        # True (bool) bypasses this check → applies in both phases (backward compat).
        if isinstance(enabled, str) and enabled != phase:
            continue

        input_key = f"damage_{tech}"
        if not hasattr(snakemake_input, input_key):
            logger.warning(
                f"damage.{tech}=true but Snakemake input '{input_key}' was not provided."
            )
            continue

        profile = (
            xr.open_dataset(getattr(snakemake_input, input_key))["profile"]
            .squeeze(drop=True)
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
        #   - time-varying column exists → renewable (multiply existing series)
        #   - only static scalar exists  → conventional, e.g. nuclear (broadcast then multiply)
        if gens.index[0] in n.generators_t.p_max_pu.columns:
            current = n.generators_t.p_max_pu[gens.index]
        else:
            static = n.generators.loc[gens.index, "p_max_pu"]
            current = pd.DataFrame(
                {g: static[g] for g in gens.index}, index=profile.index
            )

        # Map bus-level damage profile → per-generator (handles shared buses)
        gen_damage = pd.DataFrame(
            {gen: profile[bus] for gen, bus in gens["bus"].items()},
            index=profile.index,
        )

        n.generators_t.p_max_pu[gens.index] = current.multiply(gen_damage)
        logger.info(f"Applied {tech} damage profile to {len(gens)} generators.")
