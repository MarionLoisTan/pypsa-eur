import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import xarray as xr
import plotly.colors
import plotly.graph_objects as go

from _plotting_helpers import _resolve_date_range

_MARKER_SYMBOLS = ["circle", "x", "diamond", "cross", "square", "triangle-up"]
_PLOTLY_COLORS  = plotly.colors.qualitative.Plotly
_SOURCE_COLORS  = {"Actual CF": "steelblue", "Damage": "tomato"}


def _get_trace_style(label: str, year_order: list[int]) -> dict:
    if " – " in label:
        year_str, _, source_type = label.partition(" – ")
        year   = int(year_str)
        color  = _PLOTLY_COLORS[year_order.index(year) % len(_PLOTLY_COLORS)]
        symbol = "circle" if "Actual" in source_type else "x"
    else:
        color  = next((v for k, v in _SOURCE_COLORS.items() if label.startswith(k)), "grey")
        symbol = "circle"
    return {"color": color, "symbol": symbol}


# ---------------------------------------------------------------------------
# Time-series CF comparison
# ---------------------------------------------------------------------------

def plot_plant_cf_comparison(
    plant_name: str,
    snapshot_range: tuple,
    damage_df: pd.DataFrame = None,
    cf_actual: pd.DataFrame = None,
    mapping: pd.DataFrame = None,
    cf_fixed_factor: float = 0.616,
    scenarios: list[dict] | None = None,
    overlay: bool = False,
) -> go.Figure:
    """Time-series CF comparison for a single plant.

    Single-year: pass damage_df / cf_actual / mapping directly.
    Multi-year:  pass scenarios list; snapshot_range may use MM-DD format
                 (applied independently to each year) or YYYY-MM-DD.
    overlay:     if True, normalise all years to a common x-axis (year 2000).
    """
    fig = go.Figure()

    if scenarios is not None:
        year_order = sorted(s["year"] for s in scenarios)
        for s in scenarios:
            year = s["year"]
            start, end = _resolve_date_range(snapshot_range, year) or snapshot_range
            _add_cf_traces(
                fig, plant_name, start, end,
                s["damage_df"], s["cf_actual"], s["mapping"], cf_fixed_factor,
                label_suffix=f" {year}", year=year, year_order=year_order,
                overlay=overlay,
            )
    else:
        start, end = snapshot_range
        _add_cf_traces(
            fig, plant_name, start, end,
            damage_df, cf_actual, mapping, cf_fixed_factor,
        )

    fig.update_layout(
        title=f"Nuclear CF comparison — {plant_name}  [{snapshot_range[0]} → {snapshot_range[1]}]",
        xaxis_title="Time",
        yaxis=dict(title="Capacity factor", range=[0, 1.05]),
        legend=dict(x=1.01, y=1, xanchor="left"),
    )
    return fig


def _add_cf_traces(
    fig: go.Figure,
    plant_name: str,
    start: str,
    end: str,
    damage_df: pd.DataFrame,
    cf_actual: pd.DataFrame,
    mapping: pd.DataFrame,
    cf_fixed_factor: float,
    label_suffix: str = "",
    year: int | None = None,
    year_order: list[int] | None = None,
    overlay: bool = False,
) -> None:
    actual_color = "steelblue"
    damage_color = "tomato"
    if year is not None and year_order is not None:
        actual_color = _PLOTLY_COLORS[year_order.index(year) % len(_PLOTLY_COLORS)]
        damage_color = actual_color

    def _norm(idx):
        return idx.map(lambda t: t.replace(year=2000)) if overlay else idx

    unit_names = mapping.loc[mapping["Name"] == plant_name, "unit_name"]
    if not unit_names.empty:
        cf_u = cf_actual.copy()
        cf_u.index = pd.to_datetime(cf_u.index, utc=True).tz_convert(None)
        mask = cf_u["unit_name"].isin(unit_names) & (cf_u.index >= start) & (cf_u.index <= end)
        cf_u = cf_u[mask]
        if not cf_u.empty:
            pivot = cf_u.pivot_table(index=cf_u.index, columns="unit_name", values="capacity_factor")
            caps  = cf_u.groupby("unit_name")["installed_capacity_mw"].first()
            cf_agg = pivot.mul(caps).sum(axis=1) / pivot.notna().mul(caps).sum(axis=1)
            fig.add_trace(go.Scatter(
                x=_norm(cf_agg.index), y=cf_agg,
                name=f"Actual CF{label_suffix}",
                line=dict(color=actual_color),
            ))

    if plant_name in damage_df.columns:
        dmg = damage_df.loc[start:end, plant_name] * cf_fixed_factor
        if not dmg.empty:
            fig.add_trace(go.Scatter(
                x=_norm(dmg.index), y=dmg,
                name=f"Damage-adjusted CF{label_suffix}",
                line=dict(color=damage_color, dash="dot"),
            ))


# ---------------------------------------------------------------------------
# Matplotlib time-series CF comparison
# ---------------------------------------------------------------------------

def plot_plant_cf_comparison_mpl(
    plant_name: str,
    snapshot_range: tuple,
    damage_df: pd.DataFrame = None,
    cf_actual: pd.DataFrame = None,
    mapping: pd.DataFrame = None,
    cf_fixed_factor: float = 0.616,
    scenarios: list[dict] | None = None,
    overlay: bool = False,
    ax: plt.Axes | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """Matplotlib version of plot_plant_cf_comparison. Returns (fig, ax)."""
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))
    else:
        fig = ax.get_figure()

    if scenarios is not None:
        year_order = sorted(s["year"] for s in scenarios)
        for s in scenarios:
            year = s["year"]
            start, end = _resolve_date_range(snapshot_range, year) or snapshot_range
            _add_cf_traces_mpl(
                ax, plant_name, start, end,
                s["damage_df"], s["cf_actual"], s["mapping"], cf_fixed_factor,
                label_suffix=f" {year}", year=year, year_order=year_order,
                overlay=overlay,
            )
    else:
        start, end = snapshot_range
        _add_cf_traces_mpl(
            ax, plant_name, start, end,
            damage_df, cf_actual, mapping, cf_fixed_factor,
        )

    ax.set_title(f"Nuclear CF — {plant_name}  [{snapshot_range[0]} → {snapshot_range[1]}]")
    ax.set_xlabel("Time")
    ax.set_ylabel("Capacity factor")
    ax.set_ylim(0, 1.05)
    ax.legend(loc="upper left", fontsize=8)
    if overlay:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig, ax


def _add_cf_traces_mpl(
    ax: plt.Axes,
    plant_name: str,
    start: str,
    end: str,
    damage_df: pd.DataFrame,
    cf_actual: pd.DataFrame,
    mapping: pd.DataFrame,
    cf_fixed_factor: float,
    label_suffix: str = "",
    year: int | None = None,
    year_order: list[int] | None = None,
    overlay: bool = False,
) -> None:
    actual_color = "steelblue"
    damage_color = "tomato"
    if year is not None and year_order is not None:
        actual_color = _PLOTLY_COLORS[year_order.index(year) % len(_PLOTLY_COLORS)]
        damage_color = actual_color

    def _norm(idx):
        return idx.map(lambda t: t.replace(year=2000)) if overlay else idx

    # Full hourly grid for the window — reindexing onto this introduces NaN at
    # missing timestamps so matplotlib draws a gap instead of a straight line.
    full_idx = pd.date_range(start, end, freq="h")

    unit_names = mapping.loc[mapping["Name"] == plant_name, "unit_name"]
    if not unit_names.empty:
        cf_u = cf_actual.copy()
        cf_u.index = pd.to_datetime(cf_u.index, utc=True).tz_convert(None)
        mask = cf_u["unit_name"].isin(unit_names) & (cf_u.index >= start) & (cf_u.index <= end)
        cf_u = cf_u[mask]
        if not cf_u.empty:
            pivot  = cf_u.pivot_table(index=cf_u.index, columns="unit_name", values="capacity_factor")
            caps   = cf_u.groupby("unit_name")["installed_capacity_mw"].first()
            cf_agg = pivot.mul(caps).sum(axis=1) / pivot.notna().mul(caps).sum(axis=1)
            cf_agg = cf_agg.reindex(full_idx)
            ax.plot(_norm(cf_agg.index), cf_agg,
                    color=actual_color, label=f"Actual CF{label_suffix}", linewidth=0.8)

    if damage_df is not None and plant_name in damage_df.columns:
        dmg = damage_df.loc[start:end, plant_name] * cf_fixed_factor
        if not dmg.empty:
            dmg = dmg.reindex(full_idx)
            ax.plot(_norm(dmg.index), dmg,
                    color=damage_color, linestyle=":", label=f"Damage-adjusted CF{label_suffix}", linewidth=0.8)


# ---------------------------------------------------------------------------
# CF source builders
# ---------------------------------------------------------------------------

def build_cf_sources(
    damage_df: pd.DataFrame | None = None,
    cf_actual: pd.DataFrame | None = None,
    mapping: pd.DataFrame | None = None,
    cf_fixed_factor: float = 0.616,
) -> dict[str, pd.DataFrame]:
    sources = {}
    if damage_df is not None:
        sources[f"Damage-adjusted CF × {cf_fixed_factor}"] = damage_df * cf_fixed_factor
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

    Keys have the format "{year} – {source_type}", e.g. "2018 – Actual CF".
    Consumed by cf_to_temp, cf_to_temp_max, cf_change_to_temp_max.
    """
    sources = {}
    for s in scenarios:
        year = s["year"]
        per_scenario = build_cf_sources(
            damage_df=s["damage_df"],
            cf_actual=s["cf_actual"],
            mapping=s["mapping"],
            cf_fixed_factor=cf_fixed_factor,
        )
        for label, df in per_scenario.items():
            sources[f"{year} – {label}"] = df
    return sources


# ---------------------------------------------------------------------------
# CF vs temperature scatter plots
# ---------------------------------------------------------------------------

def _extract_temp(
    cutout_data: xr.Dataset | dict,
    lat: float,
    lon: float,
    start: str,
    end: str,
    year: int | None = None,
) -> pd.Series:
    cd = cutout_data[year] if isinstance(cutout_data, dict) else cutout_data
    return (
        cd["lake_s_temp"]
        .sel(x=lon, y=lat, method="nearest")
        .to_pandas()
        .loc[start:end]
    )


def cf_to_temp(
    cutout_data: xr.Dataset | dict,
    powerplants_df: pd.DataFrame,
    cf_sources: dict[str, pd.DataFrame],
    plant_name: str | None = None,
    date_range: tuple | None = None,
) -> go.Figure:
    """Scatter: hourly CF vs lake surface temperature."""
    first_source = next(iter(cf_sources.values()))
    plants = [plant_name] if plant_name is not None else list(first_source.columns)

    is_multi_year = any(" – " in lbl for lbl in cf_sources)
    year_order = sorted({int(lbl.split(" – ")[0]) for lbl in cf_sources if " – " in lbl})

    default_range = (str(first_source.index[0].date()), str(first_source.index[-1].date()))
    title_range = date_range or default_range
    fig = go.Figure()

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
            else:
                year = None
                start, end = date_range or default_range

            temp = _extract_temp(cutout_data, lat, lon, start, end, year=year)
            cf_series = source_df.loc[start:end, plant].dropna()
            aligned = pd.concat({"temp": temp, "cf": cf_series}, axis=1, join="inner").dropna()
            if aligned.empty:
                continue

            style = _get_trace_style(label, year_order)
            fig.add_trace(go.Scatter(
                x=aligned["temp"], y=aligned["cf"],
                mode="markers",
                marker=dict(symbol=style["symbol"], color=style["color"], size=5, opacity=0.6),
                name=label if plant_name is not None else f"{plant} — {label}",
            ))

    fig.update_layout(
        title=f"CF vs lake surface temperature [{title_range[0]} → {title_range[1]}]",
        xaxis_title="Lake surface temperature (K)",
        yaxis=dict(title="Capacity factor", range=[0, 1.05]),
        legend=dict(x=1.01, y=1),
    )
    return fig


def cf_to_temp_max(
    cutout_data: xr.Dataset | dict,
    powerplants_df: pd.DataFrame,
    cf_sources: dict[str, pd.DataFrame],
    plant_name: str | None = None,
    date_range: tuple | None = None,
) -> go.Figure:
    """Scatter: hourly CF deviation from annual mean vs daily max lake temperature.

    Every hour is plotted; x is the daily maximum lake temperature for that day
    (not the instantaneous temperature at that hour).
    """
    first_source = next(iter(cf_sources.values()))
    plants = [plant_name] if plant_name is not None else list(first_source.columns)

    is_multi_year = any(" – " in lbl for lbl in cf_sources)
    year_order = sorted({int(lbl.split(" – ")[0]) for lbl in cf_sources if " – " in lbl})

    default_range = (str(first_source.index[0].date()), str(first_source.index[-1].date()))
    title_range = date_range or default_range
    fig = go.Figure()

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
            else:
                year = None
                start, end = date_range or default_range

            temp = _extract_temp(cutout_data, lat, lon, start, end, year=year)
            temp_daily_max = temp.groupby(temp.index.date).transform("max")

            cf_series = source_df.loc[start:end, plant].dropna()
            cf_dev = cf_series - cf_series.mean()

            aligned = pd.concat({"temp": temp_daily_max, "cf": cf_dev}, axis=1, join="inner").dropna()
            if aligned.empty:
                continue

            style = _get_trace_style(label, year_order)
            fig.add_trace(go.Scatter(
                x=aligned["temp"], y=aligned["cf"],
                mode="markers",
                marker=dict(symbol=style["symbol"], color=style["color"], size=5, opacity=0.6),
                name=label if plant_name is not None else f"{plant} — {label}",
            ))

    fig.update_layout(
        title=f"CF vs daily max lake surface temperature [{title_range[0]} → {title_range[1]}]",
        xaxis_title="Daily max lake surface temperature (K)",
        yaxis=dict(title="CF − annual mean", range=[-1, 1]),
        legend=dict(x=1.01, y=1),
    )
    return fig


def cf_dev_to_temp_p90(
    cutout_data: xr.Dataset | dict,
    powerplants_df: pd.DataFrame,
    cf_sources: dict[str, pd.DataFrame],
    plant_name: str | None = None,
    date_range: tuple | None = None,
) -> go.Figure:
    """Scatter: (CF − annual mean) vs (daily max lake temp − annual P90 of daily max temp).

    Both axes are expressed as anomalies, isolating the response of CF to
    unusually hot days (x > 0 means above-P90 temperature).
    Pearson r is appended to each trace label.
    """
    first_source = next(iter(cf_sources.values()))
    plants = [plant_name] if plant_name is not None else list(first_source.columns)

    is_multi_year = any(" – " in lbl for lbl in cf_sources)
    year_order = sorted({int(lbl.split(" – ")[0]) for lbl in cf_sources if " – " in lbl})

    default_range = (str(first_source.index[0].date()), str(first_source.index[-1].date()))
    title_range = date_range or default_range
    fig = go.Figure()

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
            else:
                year = None
                start, end = date_range or default_range

            temp = _extract_temp(cutout_data, lat, lon, start, end, year=year)
            daily_max = temp.groupby(temp.index.date).max()
            p90 = daily_max.quantile(0.90)
            temp_anom = temp.groupby(temp.index.date).transform("max") - p90

            cf_series = source_df.loc[start:end, plant].dropna()
            cf_dev = cf_series - cf_series.mean()

            aligned = pd.concat({"temp": temp_anom, "cf": cf_dev}, axis=1, join="inner").dropna()
            if aligned.empty:
                continue

            r = aligned["temp"].corr(aligned["cf"])
            style = _get_trace_style(label, year_order)
            trace_name = label if plant_name is not None else f"{plant} — {label}"
            fig.add_trace(go.Scatter(
                x=aligned["temp"], y=aligned["cf"],
                mode="markers",
                marker=dict(symbol=style["symbol"], color=style["color"], size=5, opacity=0.6),
                name=f"{trace_name}  (r={r:.2f})",
            ))

    fig.update_layout(
        title=f"CF anomaly vs lake temp anomaly (P90) [{title_range[0]} → {title_range[1]}]",
        xaxis_title="Daily max lake temp − annual P90 (K)",
        yaxis=dict(title="CF − annual mean", range=[-1, 1]),
        legend=dict(x=1.01, y=1),
    )
    return fig


def cf_change_to_temp_max(
    cutout_data: xr.Dataset | dict,
    powerplants_df: pd.DataFrame,
    cf_sources: dict[str, pd.DataFrame],
    plant_name: str | None = None,
    date_range: tuple | None = None,
) -> go.Figure:
    """Scatter: CF change from previous peak-temp hour vs daily max lake temperature."""
    first_source = next(iter(cf_sources.values()))
    plants = [plant_name] if plant_name is not None else list(first_source.columns)

    is_multi_year = any(" – " in lbl for lbl in cf_sources)
    year_order = sorted({int(lbl.split(" – ")[0]) for lbl in cf_sources if " – " in lbl})

    default_range = (str(first_source.index[0].date()), str(first_source.index[-1].date()))
    title_range = date_range or default_range
    fig = go.Figure()

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
            else:
                year = None
                start, end = date_range or default_range

            temp = _extract_temp(cutout_data, lat, lon, start, end, year=year)
            daily_max_idx = temp.groupby(temp.index.date).idxmax()
            temp = temp.loc[daily_max_idx]

            cf_series = source_df.loc[start:end, plant].dropna()
            cf_change = cf_series.diff().reindex(temp.index)
            aligned = pd.concat({"temp": temp, "cf": cf_change}, axis=1, join="inner").dropna()
            if aligned.empty:
                continue

            style = _get_trace_style(label, year_order)
            fig.add_trace(go.Scatter(
                x=aligned["temp"], y=aligned["cf"],
                mode="markers",
                marker=dict(symbol=style["symbol"], color=style["color"], size=5, opacity=0.6),
                name=label if plant_name is not None else f"{plant} — {label}",
            ))

    fig.update_layout(
        title=f"CF change vs daily max lake surface temperature [{title_range[0]} → {title_range[1]}]",
        xaxis_title="Lake surface temperature (K)",
        yaxis=dict(title="ΔCF (from previous peak)", range=[-1, 1]),
        legend=dict(x=1.01, y=1),
    )
    return fig
