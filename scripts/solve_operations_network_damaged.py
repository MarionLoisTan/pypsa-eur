import pypsa
import xarray as xr
import pandas as pd
from pathlib import Path
from scripts._helpers import configure_logging, set_scenario_config, update_config_from_wildcards
from scripts.solve_network import prepare_network


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
    
    # Load damaged profile (hourly resolution)
    damaged_profile_ds = xr.open_dataset(snakemake.input.damaged_profile)
    damaged_profile_hourly = damaged_profile_ds['profile'].squeeze(drop=True).to_pandas()
    
    # Check if network has been time-aggregated
    network_snapshots = n.snapshots
    profile_timesteps = damaged_profile_hourly.index
    
    print(f"Network has {len(network_snapshots)} snapshots")
    print(f"Damaged profile has {len(profile_timesteps)} timesteps")
    
    # Determine if aggregation is needed
    if len(network_snapshots) < len(profile_timesteps):
        print("Network is time-aggregated. Aggregating damaged profile to match...")
        
        # Determine network temporal resolution
        if len(network_snapshots) > 1:
            time_diff = network_snapshots[1] - network_snapshots[0]
        else:
            time_diff = pd.Timedelta('1h')  # Default to hourly
        
        print(f"Detected network resolution: {time_diff}")
        
        # Resample damaged profile to match (using mean for capacity factors)
        damaged_profile = damaged_profile_hourly.resample(time_diff).mean()
        
        # Align with network snapshots (trim to match length)
        min_len = min(len(damaged_profile), len(network_snapshots))
        damaged_profile = damaged_profile.iloc[:min_len]
        damaged_profile.index = network_snapshots[:min_len]
        
        print(f"Aggregated damaged profile to {len(damaged_profile)} timesteps")
    else:
        # No aggregation needed
        damaged_profile = damaged_profile_hourly
        print("Using hourly damaged profile (no aggregation needed)")
    
    # Replace onwind p_max_pu with damaged profile
    onwind_gens = n.generators[n.generators.carrier == "onwind"]
    if len(onwind_gens) > 0:
        n.generators_t.p_max_pu[onwind_gens.index] = damaged_profile[onwind_gens.bus]
        print(f"Applied damaged profile to {len(onwind_gens)} onwind generators")
    
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
    from scripts.solve_network import collect_kwargs
    from scripts._benchmark import memory_logger

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