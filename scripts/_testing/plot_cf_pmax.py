"""
Capacity factor and p_max_pu plotting helpers for network inspection notebooks.
"""

from itertools import cycle

import matplotlib.pyplot as plt
import pandas as pd

from _plotting_helpers import (
    _DAMAGE_COLORS,
    _compute_day_ticks,
    _darken_hex,
    _hex_to_rgba,
    show_fullscreen,
)


# ---------------------------------------------------------------------------
# Data builder
# ---------------------------------------------------------------------------

def build_p_max_pu_df(
    networks: dict,
    carriers: list[str],
    by_bus: bool = False,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """
    Build a tidy DataFrame of p_max_pu time series.

    Parameters
    ----------
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → network.
    carriers : list[str]
        Internal carrier names, e.g. ``["nuclear", "onwind"]``.
    by_bus : bool
        If False, average across all generators of a carrier per scenario.
        If True, average within each bus.
    start, end : str or None
        Optional snapshot range.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex of snapshots.
        Columns: "label | carrier" or "label | carrier @ bus".
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

            # Per-generator series (static fallback for generators without time-varying p_max_pu)
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

    return pd.DataFrame(series)


# ---------------------------------------------------------------------------
# Matplotlib
# ---------------------------------------------------------------------------

def plot_p_max_pu(
    networks: dict,
    carriers: list[str],
    by_bus: bool = False,
    start: str | None = None,
    end: str | None = None,
    figsize: tuple = (8, 4),
):
    """
    Matplotlib line plot of p_max_pu. See ``build_p_max_pu_df`` for parameter docs.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    df = build_p_max_pu_df(networks, carriers, by_bus=by_bus, start=start, end=end)

    color_cycle = cycle(_DAMAGE_COLORS)
    fig, ax = plt.subplots(figsize=figsize)

    for col in df.columns:
        ax.plot(df.index, df[col], label=col, color=next(color_cycle), linewidth=1.5)

    tickvals, ticktext = _compute_day_ticks(df.index)
    ax.set_xticks(tickvals)
    ax.set_xticklabels(ticktext, rotation=30, ha="right", fontsize=8)
    ax.xaxis.set_minor_locator(plt.matplotlib.dates.HourLocator(byhour=[6, 12, 18]))

    ax.set_ylim(0, 1)
    ax.grid(True, which="major", axis="both", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.set_ylabel("p_max_pu")
    ax.set_xlabel("snapshot")
    ax.legend(loc="upper right", fontsize=8, framealpha=0.8)
    fig.tight_layout()
    return fig, ax


# ---------------------------------------------------------------------------
# Plotly
# ---------------------------------------------------------------------------

def iplot_p_max_pu(
    networks: dict,
    carriers: list[str],
    by_bus: bool = False,
    start: str | None = None,
    end: str | None = None,
    open_in_browser: bool = False,
):
    """
    Interactive Plotly line plot of p_max_pu. See ``build_p_max_pu_df`` for parameter docs.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    import plotly.express as px

    df = build_p_max_pu_df(networks, carriers, by_bus=by_bus, start=start, end=end)
    fig = px.line(df, y=df.columns.tolist(), labels={"value": "p_max_pu", "index": "snapshot"})
    if open_in_browser:
        show_fullscreen(fig)
    return fig


# ---------------------------------------------------------------------------
# CF vs p_max_pu comparison
# ---------------------------------------------------------------------------

def build_cf_pmax_df(
    ts: pd.DataFrame,
    networks: dict,
    bus: str,
    carrier: str,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """
    Build a tidy DataFrame combining capacity_factor and p_max_pu for a single
    (bus, carrier) across one or more scenarios.

    Parameters
    ----------
    ts : pd.DataFrame
        Output of ``nc.statistics.capacity_factor(groupby_time=False)``.
        MultiIndex rows: (component, network, bus, carrier); columns: timestamps.
    networks : dict[str, pypsa.Network]
        Ordered mapping of scenario label → network.
    bus : str
        Bus name to filter on, e.g. ``"FR0 0"``.
    carrier : str
        Carrier name to filter on, e.g. ``"nuclear"``.
    start, end : str or None
        Optional snapshot range.

    Returns
    -------
    pd.DataFrame
        Index: DatetimeIndex. Columns: "{label} | CF" and "{label} | p_max_pu".
    """
    series = {}

    for label, n in networks.items():
        # --- capacity factor from statistics ts ---
        try:
            cf_row = ts.loc[("Generator", label, bus, carrier)]
        except KeyError:
            import warnings
            warnings.warn(f"No CF data for (Generator, {label}, {bus}, {carrier}) — skipping.")
            continue

        cf = cf_row.copy()
        cf.index = pd.to_datetime(cf.index)

        if start is not None or end is not None:
            i, j = cf.index.slice_locs(start, end)
            cf = cf.iloc[i:j]

        series[f"{label} | CF"] = cf

        # --- p_max_pu from network generators at this bus ---
        gens = n.generators.index[
            (n.generators.carrier == carrier) & (n.generators.bus == bus)
        ]
        if len(gens) == 0:
            continue

        snapshots = cf.index
        frames = {}
        for g in gens:
            if g in n.generators_t.p_max_pu.columns:
                frames[g] = n.generators_t.p_max_pu.loc[snapshots, g]
            else:
                frames[g] = pd.Series(n.generators.loc[g, "p_max_pu"], index=snapshots)

        series[f"{label} | p_max_pu"] = pd.DataFrame(frames).mean(axis=1)

    return pd.DataFrame(series)


def plot_cf_pmax(
    ts: pd.DataFrame,
    networks: dict,
    bus: str,
    carrier: str,
    start: str | None = None,
    end: str | None = None,
    figsize: tuple = (8, 4),
):
    """
    Matplotlib comparison of capacity_factor (solid) vs p_max_pu (dashed).
    Same color per scenario. See ``build_cf_pmax_df`` for parameter docs.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    df = build_cf_pmax_df(ts, networks, bus, carrier, start=start, end=end)

    color_cycle = cycle(_DAMAGE_COLORS)
    scenario_colors = {label: next(color_cycle) for label in networks}

    fig, ax = plt.subplots(figsize=figsize)
    for col in df.columns:
        label = col.split(" | ")[0]
        is_cf = col.endswith("| CF")
        color = scenario_colors[label]
        if is_cf:
            ax.fill_between(df.index, df[col], alpha=0.25, color=color)
            ax.plot(df.index, df[col], color=color, linewidth=0.5, label=col)
        else:
            ax.plot(df.index, df[col], color=_darken_hex(color), linestyle=":", linewidth=1.5, label=col)

    tickvals, ticktext = _compute_day_ticks(df.index)
    ax.set_xticks(tickvals)
    ax.set_xticklabels(ticktext, rotation=30, ha="right", fontsize=8)
    ax.xaxis.set_minor_locator(plt.matplotlib.dates.HourLocator(byhour=[6, 12, 18]))

    ax.set_ylim(0, 1)
    ax.grid(True, which="major", axis="both", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.set_ylabel("p.u.")
    ax.set_xlabel("snapshot")
    ax.set_title(f"{carrier} @ {bus}")
    ax.legend(loc="upper right", fontsize=8, framealpha=0.8)
    fig.tight_layout()
    return fig, ax


def iplot_cf_pmax(
    ts: pd.DataFrame,
    networks: dict,
    bus: str,
    carrier: str,
    start: str | None = None,
    end: str | None = None,
    open_in_browser: bool = False,
):
    """
    Interactive Plotly comparison of capacity_factor (solid) vs p_max_pu (dashed).
    See ``build_cf_pmax_df`` for parameter docs.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    import plotly.graph_objects as go

    df = build_cf_pmax_df(ts, networks, bus, carrier, start=start, end=end)
    scenario_colors = {
        label: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)] for i, label in enumerate(networks)
    }

    fig = go.Figure()
    for col in df.columns:
        label = col.split(" | ")[0]
        is_cf = col.endswith("| CF")
        color = scenario_colors[label]
        if is_cf:
            fig.add_trace(go.Scatter(
                x=df.index, y=df[col], name=col,
                mode="lines",
                fill="tozeroy",
                fillcolor=_hex_to_rgba(color, 0.2),
                line=dict(color=color, width=1),
                connectgaps=False,
            ))
        else:
            fig.add_trace(go.Scatter(
                x=df.index, y=df[col], name=col,
                mode="lines",
                line=dict(color=_darken_hex(color), width=1.5, dash="dot"),
                connectgaps=False,
            ))

    fig.update_layout(
        hovermode="x unified",
        title=f"{carrier} @ {bus}",
        yaxis=dict(range=[0, 1], title="p.u."),
        xaxis_title="snapshot",
    )
    if open_in_browser:
        show_fullscreen(fig)
    return fig
