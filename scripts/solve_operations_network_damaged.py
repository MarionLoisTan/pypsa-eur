import logging

import pypsa
from scripts._helpers import configure_logging, set_scenario_config, update_config_from_wildcards
from scripts.build_damage_profiles._apply import apply_damage_profiles
from scripts.solve_operations_network import run_operations

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

    configure_logging(snakemake)  # pylint: disable=E0606
    set_scenario_config(snakemake)
    update_config_from_wildcards(snakemake.config, snakemake.wildcards)

    # Load the solved network (with optimized capacities from planning solve)
    n = pypsa.Network(snakemake.input.network)

    # Apply damage profiles before fixing capacities
    apply_damage_profiles(n, snakemake.params.damage, snakemake.input, phase="dispatch")

    # Add load shedding capability
    n = add_load_shedding(n)

    # Fix capacities, prepare, solve, and export
    run_operations(n, snakemake)
