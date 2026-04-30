# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT
"""
Solves linear optimal dispatch in hourly resolution using the capacities of
previous capacity expansion in rule :mod:`solve_network`.

Also used for damaged-dispatch scenarios (rules solve_operations_network_damaged_elec
and solve_operations_network_damaged_sector): when snakemake.params.damage is set,
damage profiles are applied and load shedding generators are added before solving.
"""

import logging

import numpy as np
import pypsa

from scripts._benchmark import memory_logger
from scripts._helpers import (
    configure_logging,
    set_scenario_config,
    update_config_from_wildcards,
)
from scripts.build_damage_profiles._apply import add_load_shedding, apply_damage_profiles
from scripts.solve_network import (
    collect_kwargs,
    prepare_network,
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            "solve_operations_network",
            configfiles="test/config.electricity.yaml",
            opts="",
            clusters="5",
            sector_opts="",
            planning_horizons="",
        )

    configure_logging(snakemake)  # pylint: disable=E0606
    set_scenario_config(snakemake)
    update_config_from_wildcards(snakemake.config, snakemake.wildcards)

    solve_opts = snakemake.params.options
    cf_solving = snakemake.params.solving["options"]
    planning_horizons = snakemake.wildcards.get("planning_horizons", None)

    np.random.seed(solve_opts.get("seed", 123))

    n = pypsa.Network(snakemake.input.network)
    damage_params = getattr(snakemake.params, "damage", None)

    # 1. Apply damage to time-series/availability before fixing capacities
    if damage_params:
        apply_damage_profiles(n, damage_params, snakemake.input, phase="dispatch")

    # 2. Fix capacities from previous optimization
    n.optimize.fix_optimal_capacities()

    # 3. Add load shedding AFTER fixing (new generators have no p_nom_opt and
    #    would be fixed to 0 if added before fix_optimal_capacities)
    if damage_params:
        add_load_shedding(n)

    prepare_network(
        n,
        solve_opts=cf_solving,
        foresight=snakemake.params.foresight,
        planning_horizons=planning_horizons,
        co2_sequestration_potential=snakemake.params["co2_sequestration_potential"],
        limit_max_growth=snakemake.params.get("sector", {}).get("limit_max_growth"),
    )

    rolling_horizon = cf_solving.get("rolling_horizon", False)
    mode = "rolling_horizon" if rolling_horizon else "single"

    all_kwargs, _ = collect_kwargs(
        snakemake.config,
        snakemake.params.solving,
        planning_horizons,
        log_fn=snakemake.log.solver,
        mode=mode,
    )

    logging_frequency = snakemake.config.get("solving", {}).get(
        "mem_logging_frequency", 30
    )

    with memory_logger(
        filename=getattr(snakemake.log, "memory", None), interval=logging_frequency
    ) as mem:
        if rolling_horizon:
            logger.info("Solving operations network with rolling horizon...")
            n.optimize.optimize_with_rolling_horizon(**all_kwargs)
        else:
            logger.info("Solving operations network...")
            n.optimize(**all_kwargs)

    logger.info(f"Maximum memory usage: {mem.mem_usage}")

    n.meta = dict(snakemake.config, **dict(wildcards=dict(snakemake.wildcards)))
    n.export_to_netcdf(snakemake.output.network)
