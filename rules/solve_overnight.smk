# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT


rule solve_sector_network:
    message:
        "Solving sector-coupled network with overnight investment optimization for {wildcards.clusters} clusters, {wildcards.planning_horizons} planning horizons, {wildcards.opts} electric options and {wildcards.sector_opts} sector options"
    params:
        solving=config_provider("solving"),
        foresight=config_provider("foresight"),
        co2_sequestration_potential=config_provider(
            "sector", "co2_sequestration_potential", default=200
        ),
        custom_extra_functionality=input_custom_extra_functionality,
    input:
        network=resources(
            "networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.nc"
        ),
    output:
        network=RESULTS
        + "networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.nc",
        config=RESULTS
        + "configs/config.base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.yaml",
    shadow:
        shadow_config
    log:
        solver=RESULTS
        + "logs/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_solver.log",
        memory=RESULTS
        + "logs/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_memory.log",
        python=RESULTS
        + "logs/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_python.log",
    threads: solver_threads
    resources:
        mem_mb=config_provider("solving", "mem_mb"),
        runtime=config_provider("solving", "runtime", default="6h"),
    benchmark:
        (
            RESULTS
            + "benchmarks/solve_sector_network/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}"
        )
    script:
        "../scripts/solve_network.py"

rule solve_operations_network_damaged_sector:
    params:
        options=config_provider("solving", "options"),
        solving=config_provider("solving"),
        foresight=config_provider("foresight"),
        planning_horizons=lambda w: w.planning_horizons,
        co2_sequestration_potential=config_provider(
            "sector", "co2_sequestration_potential", default=200
        ),
        custom_extra_functionality=input_custom_extra_functionality,
    input:
        network=RESULTS + "networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.nc",
        damaged_profile=ancient(resources("damage_profiles/profile_{clusters}_onwind_damaged.nc"))
    output:
        network=RESULTS + "networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_damaged-dispatch.nc",
    log:
        solver=normpath(RESULTS + "logs/solve_operations_network_damaged/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_damaged-dispatch_solver.log"),
        python=RESULTS + "logs/solve_operations_network_damaged/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_damaged-dispatch_python.log",
    benchmark:
        RESULTS + "benchmarks/solve_operations_network_damaged/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_damaged-dispatch"
    threads: 4
    resources:
        mem_mb=memory,
        runtime=config_provider("solving", "runtime", default="6h"),
    shadow:
        shadow_config
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/solve_operations_network_damaged.py"