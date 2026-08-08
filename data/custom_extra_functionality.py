# SPDX-FileCopyrightText: : 2023- The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

import pypsa


def custom_extra_functionality(n, snapshots, snakemake):
    """
    Add custom extra functionality constraints.
    """
    _add_biomass_cap(n, snakemake)
    _add_renewable_cap(n, snakemake)
    _add_nuclear_pmin(n, snakemake)
    _override_marginal_cost(n, snakemake)
    _fix_store_soc_from_base(n, snakemake)


def _fix_store_soc_from_base(n, snakemake):
    """
    For damaged dispatch runs, pin the initial and final SOC of all storage
    components to the values from the base (capacity expansion) network.

    Covers Store (H2) and StorageUnit (battery, PHS, hydro) components.
    Since the base was solved with cyclic constraints its start == end SOC,
    so these constraints are consistent with the cyclic constraint already
    in the model.
    """
    if not getattr(snakemake.params, "damage", None):
        return

    base_n = pypsa.Network(snakemake.input.network)

    # ── Stores (H2, etc.) ─────────────────────────────────────────────────────
    store_avail = [s for s in n.stores.index if s in base_n.stores_t.e.columns]
    if store_avail:
        base_e    = base_n.stores_t.e[store_avail]
        store_e   = n.model["Store-e"]
        e0 = store_e.loc[n.snapshots[0],  store_avail]
        eT = store_e.loc[n.snapshots[-1], store_avail]
        n.model.add_constraints(e0 == base_e.iloc[0],  name="store_soc_initial")
        n.model.add_constraints(eT == base_e.iloc[-1], name="store_soc_final")

    # ── StorageUnits (battery, PHS, hydro) ────────────────────────────────────
    su_avail = [
        s for s in n.storage_units.index
        if s in base_n.storage_units_t.state_of_charge.columns
    ]
    if su_avail:
        base_soc  = base_n.storage_units_t.state_of_charge[su_avail]
        su_soc    = n.model["StorageUnit-state_of_charge"]
        soc0 = su_soc.loc[n.snapshots[0],  su_avail]
        socT = su_soc.loc[n.snapshots[-1], su_avail]
        n.model.add_constraints(soc0 == base_soc.iloc[0],  name="su_soc_initial")
        n.model.add_constraints(socT == base_soc.iloc[-1], name="su_soc_final")


def _override_marginal_cost(n, snakemake):
    overrides = snakemake.config.get("custom", {}).get("marginal_cost_overrides", {})
    if not overrides:
        return

    weights = n.snapshot_weightings.objective
    for carrier, new_cost in overrides.items():
        gens_i = n.generators.query("carrier == @carrier").index
        if gens_i.empty:
            continue
        delta = float(new_cost) - n.generators.loc[gens_i, "marginal_cost"]
        p = n.model["Generator-p"].loc[:, gens_i]
        n.model.objective += (p * weights * delta).sum()
        n.generators.loc[gens_i, "marginal_cost"] = float(new_cost)


def _add_nuclear_pmin(n, snakemake):
    p_min_pu = snakemake.config.get("custom", {}).get("nuclear_p_min_pu", None)
    if p_min_pu is None:
        return

    nuc_i = n.generators.query(
        "carrier == 'nuclear' and not p_nom_extendable"
    ).index
    if nuc_i.empty:
        return

    p_nom = n.generators.loc[nuc_i, "p_nom"]
    p_max_pu_t = n.generators_t.p_max_pu.reindex(columns=nuc_i, fill_value=1.0)

    # when derated below p_min_pu, allow the plant to run at p_max_pu rather than going infeasible
    lb = p_max_pu_t.clip(upper=float(p_min_pu)).multiply(p_nom)

    p = n.model["Generator-p"].loc[:, nuc_i]
    n.model.add_constraints(p >= lb, name="nuclear_p_min")


def _add_biomass_cap(n, snakemake):
    max_cap_mw = snakemake.config.get("custom", {}).get("biomass_max_cap_mw", None)
    if max_cap_mw is None:
        return

    biomass_ext = n.generators.query(
        "carrier == 'biomass' and p_nom_extendable"
    ).index
    if biomass_ext.empty:
        return

    p_nom = n.model["Generator-p_nom"].loc[biomass_ext]
    n.model.add_constraints(p_nom.sum() <= float(max_cap_mw), name="biomass_total_cap")


def _add_renewable_cap(n, snakemake):
    """
    Absolute national capacity caps for solar, onshore wind, and offshore wind,
    independent of expansion_limit.

    Config keys (all optional):
      custom:
        solar_max_cap_mw: 100000    # combined solar + solar-hsat
        onwind_max_cap_mw: 50000    # onwind only
        offwind_max_cap_mw: 30000   # combined offwind-ac + offwind-dc
    """
    custom = snakemake.config.get("custom", {})

    groups = {
        "solar_max_cap_mw": ["solar", "solar-hsat"],
        "onwind_max_cap_mw": ["onwind"],
        "offwind_max_cap_mw": ["offwind-ac", "offwind-dc"],
    }

    for key, carriers in groups.items():
        max_cap = custom.get(key, None)
        if max_cap is None:
            continue

        gens = n.generators.query(
            "carrier in @carriers and p_nom_extendable"
        ).index
        if gens.empty:
            continue

        p_nom = n.model["Generator-p_nom"].loc[gens]
        n.model.add_constraints(
            p_nom.sum() <= float(max_cap),
            name=key.replace("_max_cap_mw", "_total_cap"),
        )
