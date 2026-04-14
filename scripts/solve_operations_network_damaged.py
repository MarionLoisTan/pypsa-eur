import logging

import pypsa
from scripts._helpers import configure_logging, set_scenario_config, update_config_from_wildcards
from scripts.solve_network import prepare_network

from scripts.build_damage_profiles._apply import apply_damage_profiles
from scripts.solve_network import collect_kwargs
from scripts._benchmark import memory_logger

logger = logging.getLogger(__name__)


def add_load_shedding(n):
    """Add load shedding - detects if electricity-only or sector-coupled."""
    has_heat = any(n.buses.carrier.str.contains("heat", na=False))
    
    if has_heat:
        # Sector-coupled load shedding
        nodes_LV = n.buses.query('carrier == "low voltage"').index
        nodes_heat1 = n.buses.query('carrier == "rural heat"').index
        nodes_heat2 = n.buses.query('carrier == "urban central heat"').index
        nodes_heat3 = n.buses.query('carrier == "urban decentral heat"').index
        
        n.add("Carrier", "load_el")
        n.add("Carrier", "load_heat")
        
        # Add load shedding generators for low voltage buses
        for bus in nodes_LV:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier='load_el',
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
        
        # Add load shedding for heat buses
        for bus in nodes_heat1:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier='load_heat',
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
        
        for bus in nodes_heat2:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier='load_heat',
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
        
        for bus in nodes_heat3:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier='load_heat',
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
    else:
        # Electricity-only load shedding
        n.add("Carrier", "load_shedding")
        for bus in n.buses.index:
            n.add("Generator",
                  bus + " load shedding",
                  bus=bus,
                  carrier='load_shedding',
                  marginal_cost=1e4,
                  p_nom_extendable=True,
                  capital_cost=0)
    
    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake
        
        snakemake = mock_snakemake(
            "solve_operations_network_damaged_elec",
            configfiles="config/test/config.test-atlite.yaml",
            clusters="5",
            opts="",
        )
    
    # Make snakemake available globally for solve_network
    import scripts.solve_network as sn_module
    sn_module.snakemake = snakemake 

    configure_logging(snakemake)
    set_scenario_config(snakemake)
    update_config_from_wildcards(snakemake.config, snakemake.wildcards)
    
    # Load the solved network (with optimized capacities from normal profile)
    n = pypsa.Network(snakemake.input.network)

    apply_damage_profiles(n, snakemake.params.damage, snakemake.input, phase="dispatch")
    
    # Fix all capacities (p_nom, e_nom, etc.) - dispatch only
    n.optimize.fix_optimal_capacities()
    print("Fixed all optimal capacities for dispatch-only optimization")
    
    # Add load shedding capability
    n = add_load_shedding(n)
    print("Added load shedding capability")
    
    # Get solve options
    solve_opts = snakemake.params.solving["options"]
    planning_horizons = snakemake.params.planning_horizons

    # Prepare network for solving
    prepare_network(
        n,
        solve_opts=solve_opts,
        foresight=snakemake.params.foresight,
        planning_horizons=planning_horizons,
        co2_sequestration_potential=snakemake.params.co2_sequestration_potential,
    )
    print("Network prepared for solving")

    # Solve the network (dispatch only)
    rolling_horizon = solve_opts.get("rolling_horizon", False)
    mode = "rolling_horizon" if rolling_horizon else "single"

    all_kwargs, _ = collect_kwargs(
        snakemake.config,
        snakemake.params.solving,
        planning_horizons,
        log_fn=snakemake.log.solver,
        mode=mode,
    )

    logging_frequency = snakemake.config.get("solving", {}).get("mem_logging_frequency", 30)

    logger.info("Solving damaged dispatch network...")
    
    with memory_logger(filename=getattr(snakemake.log, "memory", None), interval=logging_frequency) as mem:
        if rolling_horizon:
            n.optimize.optimize_with_rolling_horizon(**all_kwargs)
        else:
            n.optimize(**all_kwargs)

    logger.info(f"Maximum memory usage: {mem.mem_usage}")
    logger.info("Network solved successfully")
    print("Network solved successfully")
    
    # Export
    n.meta = dict(snakemake.config, **dict(wildcards=dict(snakemake.wildcards)))
    n.export_to_netcdf(snakemake.output.network)
    print(f"Exported to {snakemake.output.network}")