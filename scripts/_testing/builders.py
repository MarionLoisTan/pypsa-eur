"""
Pure-data builders for the _testing plotting scripts.

No matplotlib or plotly imports — all functions return plain DataFrames
or dicts of DataFrames.

Sections
--------
1. CF / damage workflow  (ENTSO-E actual CF + nuclear damage profiles)
2. Network / PyPSA workflow  (p_max_pu, CF vs p_max_pu, net flows)
3. Statistics  (n.statistics() outputs)
"""

import warnings

import numpy as np
import pandas as pd

from scripts.build_damage_profiles.build_nuclear_damage_profiles import extract_lake_temp


# ===========================================================================
# 1 — CF / damage workflow
# ===========================================================================

def _consecutive_groups(missing: pd.DatetimeIndex) -> list[pd.DatetimeIndex]:
    """Split a sorted DatetimeIndex of missing timestamps into consecutive runs."""
    if len(missing) == 0:
        return []
    s = pd.Series(missing)
    group_ids = (s.diff().dt.total_seconds().div(3600) > 1).cumsum()
    return [pd.DatetimeIndex(s[group_ids == g]) for g in group_ids.unique()]


def fill_short_gaps(
    cf: pd.DataFrame,
    max_gap_hours: int = 168,
    interp_threshold: int = 4,
) -> pd.DataFrame:
    """Fill short gaps in ENTSO-E actual CF data.

    Gaps shorter than ``interp_threshold`` hours are filled by averaging
    neighbouring values; gaps up to ``max_gap_hours`` are set to zero
    (modelled as forced outages); gaps at or above ``max_gap_hours`` are
    left as-is (treated as missing data, not outages).

    Parameters
    ----------
    cf : pd.DataFrame
        Long-format ENTSO-E table with columns ``unit_name``,
        ``capacity_factor``, ``generation_mw``, ``country``, ``unit_code``,
        ``psr_type``, ``installed_capacity_mw``.
    max_gap_hours : int
        Gaps at or above this length are not filled (default 168 = 1 week).
    interp_threshold : int
        Gaps strictly below this length are filled by neighbour averaging
        rather than set to zero (default 4).

    Returns
    -------
    pd.DataFrame
        Same schema as ``cf``, tz-naive UTC index, with added rows for
        filled timestamps.
    """
    idx_utc = pd.to_datetime(cf.index, utc=True).tz_convert(None)
    cf_norm = cf.copy()
    cf_norm.index = idx_utc

    expected = pd.date_range(start=idx_utc.min(), end=idx_utc.max(), freq="h")

    new_rows = []
    for unit, group in cf_norm.groupby("unit_name"):
        missing = expected.difference(group.index)
        if len(missing) == 0:
            continue
        meta = group.iloc[0][
            ["country", "unit_name", "unit_code", "psr_type", "installed_capacity_mw"]
        ].to_dict()
        for gap in _consecutive_groups(missing):
            if len(gap) >= max_gap_hours:
                continue
            if len(gap) < interp_threshold:
                before = group[group.index < gap[0]]
                after  = group[group.index > gap[-1]]
                if not before.empty and not after.empty:
                    cf_val  = (before.iloc[-1]["capacity_factor"] + after.iloc[0]["capacity_factor"]) / 2
                    gen_val = (before.iloc[-1]["generation_mw"]   + after.iloc[0]["generation_mw"])   / 2
                elif not before.empty:
                    cf_val, gen_val = before.iloc[-1]["capacity_factor"], before.iloc[-1]["generation_mw"]
                elif not after.empty:
                    cf_val, gen_val = after.iloc[0]["capacity_factor"],   after.iloc[0]["generation_mw"]
                else:
                    cf_val, gen_val = 0.0, 0.0
            else:
                cf_val, gen_val = 0.0, 0.0

            for ts in gap:
                new_rows.append({**meta, "generation_mw": gen_val, "capacity_factor": cf_val, "__ts": ts})

    if not new_rows:
        return cf_norm

    fill_df = pd.DataFrame(new_rows).set_index("__ts")
    fill_df.index.name = cf.index.name
    return pd.concat([cf_norm, fill_df]).sort_index()


def _resolve_date_range(
    date_range: tuple[str, str] | None,
    year: int,
) -> tuple[str, str] | None:
    """Expand MM-DD date strings to YYYY-MM-DD for the given year.

    Returns None if date_range is None or already in YYYY-MM-DD format,
    so callers can use ``_resolve_date_range(...) or fallback``.
    """
    if date_range is None:
        return None
    start, end = date_range
    if len(start) == 5 and start[2] == "-":
        return (f"{year}-{start}", f"{year}-{end}")
    return None


def _get_plant_row(plant_name: str, powerplants_df: pd.DataFrame) -> pd.Series:
    mask = powerplants_df["Name"] == plant_name
    if not mask.any():
        raise KeyError(f"Plant '{plant_name}' not found in powerplants_df.")
    return powerplants_df.loc[mask].iloc[0]


def _plant_temp_c(
    plant_row: pd.Series,
    cutout_data,
    time_index: pd.DatetimeIndex,
) -> np.ndarray:
    raw = extract_lake_temp(cutout_data, plant_row["lat"], plant_row["lon"], time_index)
    return raw - 273.0


def build_cf_sources(
    damage_df: pd.DataFrame | None = None,
    cf_actual: pd.DataFrame | None = None,
    mapping: pd.DataFrame | None = None,
    cf_fixed_factor: float = 0.616,
) -> dict[str, pd.DataFrame]:
    """Build a sources dict mapping label → wide-format CF DataFrame.

    Parameters
    ----------
    damage_df : pd.DataFrame or None
        Damage profile (cols=plants, index=timestamps, values∈[0,1]).
        Damage-adjusted CF is ``cf_fixed_factor - damage_df``.
    cf_actual : pd.DataFrame or None
        Long-format ENTSO-E actual CF table.  Requires ``mapping``.
    mapping : pd.DataFrame or None
        Columns ``Name`` and ``unit_name`` — maps plant names to ENTSO-E units.
    cf_fixed_factor : float
        Baseline load factor; damage is subtracted from this (default 0.616).

    Returns
    -------
    dict[str, pd.DataFrame]
        Keys: ``"Damage-adjusted CF"``, ``"Actual CF"`` (whichever supplied).
        Each value is a wide DataFrame with plants as columns.
    """
    sources = {}
    if damage_df is not None:
        sources["Damage-adjusted CF"] = cf_fixed_factor - damage_df
    if cf_actual is not None and mapping is not None:
        cf_u = cf_actual.copy()
        cf_u.index = pd.to_datetime(cf_u.index, utc=True).tz_convert(None)
        wide_rows = {}
        for plant in mapping["Name"].unique():
            unit_names = mapping.loc[mapping["Name"] == plant, "unit_name"]
            sub = cf_u[cf_u["unit_name"].isin(unit_names)]
            if sub.empty:
                continue
            pivot = sub.pivot_table(index=sub.index, columns="unit_name", values="capacity_factor")
            caps = sub.groupby("unit_name")["installed_capacity_mw"].first()
            wide_rows[plant] = pivot.mul(caps).sum(axis=1) / pivot.notna().mul(caps).sum(axis=1)
        if wide_rows:
            sources["Actual CF"] = pd.DataFrame(wide_rows)
    return sources


def build_multi_scenario_sources(
    scenarios: list[dict],
    cf_fixed_factor: float = 0.616,
) -> dict[str, pd.DataFrame]:
    """Build a unified CF sources dict from multiple scenarios, prefixed by year.

    Keys have the format ``"{year} – {source_type}"``, e.g. ``"2018 – Actual CF"``.
    Consumed by ``iplot_cf_temp`` in ``plot_cf.py``.

    Parameters
    ----------
    scenarios : list[dict]
        Each dict must contain ``year``, and optionally ``damage_df``,
        ``cf_actual``, and ``mapping``.
    cf_fixed_factor : float
        Forwarded to ``build_cf_sources`` (default 0.616).
    """
    sources = {}
    for s in scenarios:
        year = s["year"]
        per_scenario = build_cf_sources(
            damage_df=s.get("damage_df"),
            cf_actual=s.get("cf_actual"),
            mapping=s.get("mapping"),
            cf_fixed_factor=cf_fixed_factor,
        )
        for label, df in per_scenario.items():
            sources[f"{year} – {label}"] = df
    return sources


def build_plant_cf_df(
    plant_name: str,
    date_range: tuple[str, str],
    *,
    damage_df: pd.DataFrame | None = None,
    cf_actual: pd.DataFrame | None = None,
    mapping: pd.DataFrame | None = None,
    cf_fixed_factor: float = 0.616,
    scenarios: list[dict] | None = None,
    overlay: bool = False,
) -> pd.DataFrame:
    """Build a wide hourly CF DataFrame for a single plant.

    Parameters
    ----------
    plant_name : str
    date_range : tuple[str, str]
        ``(start, end)`` ISO strings. MM-DD format is accepted for multi-year
        scenarios and expanded to ``YYYY-MM-DD`` per year.
    damage_df, cf_actual, mapping : optional
        Single-year raw inputs.  Pass ``scenarios`` instead for multi-year.
    cf_fixed_factor : float
        Baseline load factor; damage is subtracted from this (default 0.616).
    scenarios : list[dict] or None
        Multi-year dicts with ``year`` and optional ``damage_df``, ``cf_actual``,
        ``mapping`` keys.
    overlay : bool
        If True, remap all timestamps to year 2000 for visual alignment.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex (hourly). Columns: ``"Actual CF"`` /
        ``"Damage-adjusted CF"`` for single year, ``"Actual CF 2018"`` etc.
        for multi-year.  NaN at timestamps with no data (draws gaps in mpl).
    """
    def _single_year(start, end, dmg_df, cf_act, mp, suffix=""):
        full_idx = pd.date_range(start, end, freq="h")
        out = {}
        if cf_act is not None and mp is not None:
            unit_names = mp.loc[mp["Name"] == plant_name, "unit_name"]
            if not unit_names.empty:
                cf_u = cf_act.copy()
                cf_u.index = pd.to_datetime(cf_u.index, utc=True).tz_convert(None)
                mask = (cf_u["unit_name"].isin(unit_names) &
                        (cf_u.index >= start) & (cf_u.index <= end))
                cf_u = cf_u[mask]
                if not cf_u.empty:
                    pivot = cf_u.pivot_table(index=cf_u.index, columns="unit_name", values="capacity_factor")
                    caps = cf_u.groupby("unit_name")["installed_capacity_mw"].first()
                    cf_agg = pivot.mul(caps).sum(axis=1) / pivot.notna().mul(caps).sum(axis=1)
                    out[f"Actual CF{suffix}"] = cf_agg.reindex(full_idx)
        if dmg_df is not None and plant_name in dmg_df.columns:
            dmg = (cf_fixed_factor - dmg_df.loc[start:end, plant_name]).reindex(full_idx)
            out[f"Damage-adjusted CF{suffix}"] = dmg
        return out

    all_series: dict = {}
    if scenarios is not None:
        for s in scenarios:
            year = s["year"]
            start, end = _resolve_date_range(date_range, year) or date_range
            all_series.update(_single_year(
                start, end,
                s.get("damage_df"), s.get("cf_actual"), s.get("mapping"),
                suffix=f" {year}",
            ))
    else:
        start, end = date_range
        all_series.update(_single_year(start, end, damage_df, cf_actual, mapping))

    if not all_series:
        return pd.DataFrame()

    df = pd.DataFrame(all_series)
    if overlay:
        df.index = df.index.map(lambda t: t.replace(year=2000))
    return df


def build_plant_damage_df(
    plant_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a DataFrame with ``availability`` and ``temp_c`` for a single plant.

    Parameters
    ----------
    plant_name : str
        Must be a column in ``damage_df`` and a ``Name`` in ``powerplants_df``.
    damage_df : pd.DataFrame
        Damage profile (index=timestamps, cols=plant names, values∈[0,1]).
    cutout_data : xr.Dataset
        ERA5 cutout with ``lake_s_temp`` variable.
    powerplants_df : pd.DataFrame
        Must have columns ``Name``, ``lat``, ``lon``.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex from ``damage_df``.
        Columns: ``availability`` (= 1 − damage), ``temp_c``.
    """
    plant = _get_plant_row(plant_name, powerplants_df)
    time_index = damage_df.index
    temp_c = _plant_temp_c(plant, cutout_data, time_index)
    return pd.DataFrame(
        {"availability": 1 - damage_df[plant_name].values, "temp_c": temp_c},
        index=time_index,
    )


def build_bus_damage_df(
    bus_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    mode: str = "aggregate",
) -> pd.DataFrame:
    """Build a DataFrame of availability and temperature for all plants at a bus.

    Parameters
    ----------
    bus_name : str
        Must match the ``bus`` column in ``powerplants_df``.
    damage_df : pd.DataFrame
        Damage profile.
    cutout_data : xr.Dataset
        ERA5 cutout with ``lake_s_temp`` variable.
    powerplants_df : pd.DataFrame
        Must have columns ``Name``, ``bus``, ``lat``, ``lon``, ``Capacity``.
    mode : str
        ``"aggregate"`` — single capacity-weighted ``availability`` column.
        ``"individual"`` — one column per plant, named by plant name.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex from ``damage_df``.
        Aggregate columns: ``availability``, ``temp_min``, ``temp_max``, ``temp_mean``.
        Individual columns: ``{plant_name}``, …, ``temp_min``, ``temp_max``, ``temp_mean``.
    """
    if mode not in ("individual", "aggregate"):
        raise ValueError("mode must be 'individual' or 'aggregate'.")

    bus_plants = powerplants_df[
        (powerplants_df["bus"] == bus_name) &
        (powerplants_df["Name"].isin(damage_df.columns))
    ].copy()
    if bus_plants.empty:
        raise ValueError(f"No plants found for bus '{bus_name}' in damage_df.")

    time_index = damage_df.index
    capacities = bus_plants["Capacity"].values.astype(float)
    profiles = np.stack(
        [damage_df[row["Name"]].values for _, row in bus_plants.iterrows()], axis=1
    )
    temps_c = np.stack(
        [_plant_temp_c(row, cutout_data, time_index) for _, row in bus_plants.iterrows()],
        axis=1,
    )

    data: dict = {}
    if mode == "individual":
        for i, (_, row) in enumerate(bus_plants.iterrows()):
            data[row["Name"]] = 1 - profiles[:, i]
    else:
        data["availability"] = 1 - np.average(profiles, axis=1, weights=capacities)

    data["temp_min"]  = temps_c.min(axis=1)
    data["temp_max"]  = temps_c.max(axis=1)
    data["temp_mean"] = temps_c.mean(axis=1)
    return pd.DataFrame(data, index=time_index)


def build_cf_temp_aligned_df(
    cf_sources: dict[str, pd.DataFrame],
    cutout_data,
    powerplants_df: pd.DataFrame,
    plant_name: str | None = None,
    date_range: tuple | None = None,
) -> pd.DataFrame:
    """Build a tidy long-format DataFrame aligning CF with lake temperature.

    Consumed by ``iplot_cf_temp`` in ``plot_cf.py``.

    Parameters
    ----------
    cf_sources : dict[str, pd.DataFrame]
        Output of ``build_cf_sources`` or ``build_multi_scenario_sources``.
        Keys with format ``"{year} – {source}"`` trigger multi-year handling.
    cutout_data : xr.Dataset or dict[int, xr.Dataset]
        ERA5 cutout (single year or dict keyed by year integer).
    powerplants_df : pd.DataFrame
        Must have columns ``Name``, ``lat``, ``lon``.
    plant_name : str or None
        Restrict to one plant; all plants if None.
    date_range : tuple or None
        ``(start, end)`` ISO strings.  MM-DD accepted for multi-year sources.

    Returns
    -------
    pd.DataFrame
        Columns: ``plant``, ``label``, ``temp``, ``cf``.
    """
    first_source = next(iter(cf_sources.values()))
    plants = [plant_name] if plant_name is not None else list(first_source.columns)
    is_multi_year = any(" – " in lbl for lbl in cf_sources)
    default_range = (str(first_source.index[0].date()), str(first_source.index[-1].date()))

    chunks = []
    for plant in plants:
        plant_row = powerplants_df.loc[powerplants_df["Name"] == plant]
        if plant_row.empty:
            continue
        lat, lon = plant_row.iloc[0]["lat"], plant_row.iloc[0]["lon"]

        for label, source_df in cf_sources.items():
            if plant not in source_df.columns:
                continue

            if is_multi_year:
                year = int(label.split(" – ")[0])
                start, end = _resolve_date_range(date_range, year) or default_range
                cd = cutout_data[year] if isinstance(cutout_data, dict) else cutout_data
            else:
                start, end = date_range or default_range
                cd = cutout_data

            temp = (
                cd["lake_s_temp"]
                .sel(x=lon, y=lat, method="nearest")
                .to_pandas()
                .loc[start:end]
            )
            cf_series = source_df.loc[start:end, plant].dropna()
            aligned = pd.concat({"temp": temp, "cf": cf_series}, axis=1, join="inner").dropna()
            if aligned.empty:
                continue

            chunk = aligned.copy()
            chunk["plant"] = plant
            chunk["label"] = label
            chunks.append(chunk[["plant", "label", "temp", "cf"]])

    if not chunks:
        return pd.DataFrame(columns=["plant", "label", "temp", "cf"])
    return pd.concat(chunks).reset_index(drop=True)


# ===========================================================================
# 2 — Network / PyPSA workflow
# ===========================================================================

def build_p_max_pu_df(
    networks: dict,
    carriers: list[str],
    by_bus: bool = False,
    start: str | None = None,
    end: str | None = None,
    overlay: bool = False,
) -> pd.DataFrame:
    """Build a tidy DataFrame of p_max_pu time series.

    Parameters
    ----------
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → network.
    carriers : list[str]
        Internal carrier names, e.g. ``["nuclear", "onwind"]``.
    by_bus : bool
        If False, capacity-weighted average across all generators of a carrier
        per scenario.  If True, average within each bus.
    start, end : str or None
        Optional snapshot range.
    overlay : bool
        If True, remap all timestamps to year 2000 for visual alignment.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex of snapshots.
        Columns: ``"{label} | {carrier}"`` or ``"{label} | {carrier} @ {bus}"``.
    """
    series = {}

    for label, n in networks.items():
        snapshots = n.snapshots
        if start is not None or end is not None:
            i, j = snapshots.slice_locs(start, end)
            snapshots = snapshots[i:j]

        for carrier in carriers:
            gens = n.generators.index[n.generators.carrier == carrier]
            if len(gens) == 0:
                continue

            frames = {}
            for g in gens:
                if g in n.generators_t.p_max_pu.columns:
                    frames[g] = n.generators_t.p_max_pu.loc[snapshots, g]
                else:
                    frames[g] = pd.Series(n.generators.loc[g, "p_max_pu"], index=snapshots)
            gen_df = pd.DataFrame(frames)

            def _weighted(df_slice, gen_names):
                w = n.generators.loc[gen_names, "p_nom_opt"].clip(lower=0)
                if w.sum() == 0:
                    w = n.generators.loc[gen_names, "p_nom"]
                return df_slice.mul(w, axis=1).sum(axis=1) / w.sum()

            if not by_bus:
                series[f"{label} | {carrier}"] = _weighted(gen_df, gen_df.columns.tolist())
            else:
                buses = n.generators.loc[gens, "bus"]
                for bus, bus_gens in buses.groupby(buses):
                    bus_cols = [g for g in bus_gens.index if g in gen_df.columns]
                    series[f"{label} | {carrier} @ {bus}"] = _weighted(gen_df[bus_cols], bus_cols)

    df = pd.DataFrame(series)
    if overlay:
        df.index = df.index.map(lambda t: t.replace(year=2000))
    return df


def build_cf_pmax_df(
    ts: pd.DataFrame,
    networks: dict,
    bus: str,
    carrier: str,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Build a tidy DataFrame combining capacity_factor and p_max_pu for a
    single (bus, carrier) across one or more scenarios.

    Parameters
    ----------
    ts : pd.DataFrame
        Output of ``n.statistics.capacity_factor(groupby_time=False)`` with
        ``nice_names=False``.
        MultiIndex rows: (component, network, bus, carrier); columns: timestamps.
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → network.
    bus : str
        Bus name, e.g. ``"FR0 0"``.
    carrier : str
        Carrier name, e.g. ``"nuclear"``.
    start, end : str or None
        Optional snapshot range.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex. Columns: ``"{label} | CF"`` and ``"{label} | p_max_pu"``.
    """
    series = {}

    for label, n in networks.items():
        try:
            cf_row = ts.loc[("Generator", label, bus, carrier)]
        except KeyError:
            warnings.warn(f"No CF data for (Generator, {label}, {bus}, {carrier}) — skipping.")
            continue

        cf = cf_row.copy()
        cf.index = pd.to_datetime(cf.index)

        if start is not None or end is not None:
            i, j = cf.index.slice_locs(start, end)
            cf = cf.iloc[i:j]

        series[f"{label} | CF"] = cf

        gens = n.generators.index[
            (n.generators.carrier == carrier) & (n.generators.bus == bus)
        ]
        if len(gens) == 0:
            continue

        net_snapshots = (
            n.generators_t.p_max_pu.index
            if not n.generators_t.p_max_pu.empty
            else n.snapshots
        )
        snapshots = net_snapshots[net_snapshots.isin(cf.index)]
        frames = {}
        for g in gens:
            if g in n.generators_t.p_max_pu.columns:
                frames[g] = n.generators_t.p_max_pu.loc[snapshots, g]
            else:
                frames[g] = pd.Series(n.generators.loc[g, "p_max_pu"], index=snapshots)

        gen_df = pd.DataFrame(frames)
        w = n.generators.loc[gens, "p_nom_opt"].clip(lower=0)
        if w.sum() == 0:
            w = n.generators.loc[gens, "p_nom"]
        series[f"{label} | p_max_pu"] = gen_df.mul(w, axis=1).sum(axis=1) / w.sum()

    return pd.DataFrame(series)


def build_agg_cf_pmax_df(
    networks: dict,
    carrier: str,
    bus: str | None = None,
    freq: str = "MS",
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Aggregated capacity-weighted CF and p_max_pu per scenario for one carrier.

    CF is computed directly from ``generators_t.p`` with the same capacity
    weights as p_max_pu, ensuring CF ≤ p_max_pu holds by construction.

    Parameters
    ----------
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → network.
    carrier : str
        Carrier name, e.g. ``"onwind"``.
    bus : str or None
        If given, restrict to a single bus; otherwise fleet-wide average.
    freq : str
        Pandas resample frequency (``"MS"`` monthly, ``"W"`` weekly) or
        ``"total"`` for a single aggregate over the full period.
    start, end : str or None
        Optional ISO date strings to restrict the time window before aggregating.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex at the requested frequency (single row for ``freq="total"``).
        Columns: ``"{label} | CF"`` and ``"{label} | p_max_pu"`` per scenario.
    """
    p_max_hourly = build_p_max_pu_df(networks, carriers=[carrier], by_bus=bus is not None)

    series = {}
    for label, n in networks.items():
        if bus is not None:
            gens = n.generators.index[
                (n.generators.carrier == carrier) & (n.generators.bus == bus)
            ]
        else:
            gens = n.generators.index[n.generators.carrier == carrier]

        if len(gens) == 0:
            warnings.warn(f"No {carrier} generators for label={label}, bus={bus} — skipping.")
            continue

        w = n.generators.loc[gens, "p_nom_opt"].clip(lower=0)
        if w.sum() == 0:
            w = n.generators.loc[gens, "p_nom"]

        p_t = n.generators_t.p[gens].loc[start:end]
        cf_hourly = p_t.sum(axis=1) / w.sum()

        p_col = f"{label} | {carrier} @ {bus}" if bus is not None else f"{label} | {carrier}"
        pmax_hourly_col = p_max_hourly[p_col].loc[start:end] if p_col in p_max_hourly.columns else None

        if freq == "total":
            series[f"{label} | CF"] = pd.Series(
                [cf_hourly.mean()], index=[cf_hourly.index[0]]
            )
            if pmax_hourly_col is not None:
                series[f"{label} | p_max_pu"] = pd.Series(
                    [pmax_hourly_col.mean()], index=[pmax_hourly_col.index[0]]
                )
        else:
            series[f"{label} | CF"] = cf_hourly.resample(freq).mean()
            if pmax_hourly_col is not None:
                series[f"{label} | p_max_pu"] = pmax_hourly_col.resample(freq).mean()

    return pd.DataFrame(series)


def build_bus_net_flow_df(
    networks: dict,
    buses: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Build a tidy DataFrame of net AC line export per bus over time.

    Net export: Σ p0[l,t] for lines where bus==bus0 minus Σ p0[l,t] for
    lines where bus==bus1.  Positive = exporting, negative = importing.

    Parameters
    ----------
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → solved network.
    buses : list[str] or None
        Buses to include; all connected buses if None.
    start, end : str or None
        Optional ISO-format snapshot range.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex. Columns: ``"{label} | {bus}"``.
        Values: net export in MW.
    """
    series = {}

    for label, n in networks.items():
        if n.lines.empty:
            continue

        snapshots = n.snapshots
        if start is not None or end is not None:
            i, j = snapshots.slice_locs(start, end)
            snapshots = snapshots[i:j]

        p0 = n.lines_t.p0.loc[snapshots]

        connected = pd.Index(n.lines.bus0.tolist() + n.lines.bus1.tolist()).unique()
        bus_list  = buses if buses is not None else sorted(connected.tolist())

        for bus in bus_list:
            lines_from = n.lines.index[n.lines.bus0 == bus]
            lines_to   = n.lines.index[n.lines.bus1 == bus]

            net = pd.Series(0.0, index=snapshots)
            if len(lines_from):
                net += p0[lines_from].sum(axis=1)
            if len(lines_to):
                net -= p0[lines_to].sum(axis=1)

            series[f"{label} | {bus}"] = net

    return pd.DataFrame(series)


# ===========================================================================
# 3 — Statistics
# ===========================================================================

def compute_stats_diff(stats_df: pd.DataFrame, base: str = "base") -> pd.DataFrame:
    """Append ``{col}_diff`` columns showing the difference relative to a base network.

    Parameters
    ----------
    stats_df : pd.DataFrame
        Output of ``n.statistics(...)``, called with ``nice_names=False``.
        Must have a MultiIndex containing a level with the base label.
    base : str
        Network label to use as the baseline (default ``"base"``).

    Returns
    -------
    pd.DataFrame
        Original DataFrame plus ``{col}_diff`` columns.
        Base-network rows have diff = 0; others show value − base_value.
        Combinations missing from the base will be NaN.
    """
    level = next(
        (i for i, vals in enumerate(stats_df.index.levels) if base in vals),
        None,
    )
    if level is None:
        raise ValueError(f"Base label {base!r} not found in any index level.")

    base_slice = stats_df.xs(base, level=level)

    aligned_base = base_slice.reindex(stats_df.index.droplevel(level)).fillna(0)
    aligned_base.index = stats_df.index

    diff = stats_df - aligned_base

    result = stats_df.copy()
    for col in stats_df.columns:
        result[col.lower().replace(" ", "_") + "_diff"] = diff[col]

    return result


def format_stats_df(
    df: pd.DataFrame,
    index_names: list[str] = ("techgroup", "network", "bus", "tech"),
) -> pd.DataFrame:
    """Rename MultiIndex levels and lowercase all column names.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ``n.statistics(...)`` or ``compute_stats_diff(...)``.
        Use ``nice_names=False`` when calling ``n.statistics()`` to get
        consistent carrier names.
    index_names : list[str]
        Names to assign to the index levels, in order.

    Returns
    -------
    pd.DataFrame
        Flattened DataFrame with renamed index levels and lowercase columns.
    """
    result = df.copy()
    result.index.names = list(index_names)
    result.columns = [c.lower() for c in result.columns]
    return result.reset_index()
