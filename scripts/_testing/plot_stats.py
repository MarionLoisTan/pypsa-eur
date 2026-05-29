from itertools import cycle

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk

from styles import _DAMAGE_COLORS
from builders import compute_stats_diff, format_stats_df


def iplot_stats_bar(
    df: pd.DataFrame,
    *,
    column: str,
    networks: list[str],
    buses: list[str] | None = None,
    color_map: dict | None = None,
) -> go.Figure:
    """
    Interactive stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df        : formatted statistics DataFrame (output of ``format_stats_df``).
    column    : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks  : scenario labels to include (one facet panel per network).
    buses     : optional bus filter; all buses shown if None.
    color_map : optional dict mapping technology name to hex color. Pass
                ``n.carriers.color.to_dict()`` to use PyPSA carrier colors.
                Falls back to ``_DAMAGE_COLORS`` cycling if None.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    data = df[df["network"].isin(networks)].copy()
    data = data[data[column] != 0]
    if buses is not None:
        data = data[data["bus"].isin(buses)]

    if color_map is None:
        techs = data["tech"].unique()
        color_map = {t: c for t, c in zip(techs, cycle(_DAMAGE_COLORS))}

    fig = px.bar(
        data,
        x="bus",
        y=column,
        color="tech",
        facet_col="network",
        barmode="relative",
        labels={column: column, "bus": "Bus"},
        color_discrete_map=color_map,
    )
    fig.update_xaxes(tickangle=30, matches=None)
    fig.add_hline(y=0, line_width=0.8, line_color="black")
    return fig


def plot_stats_bar(
    df: pd.DataFrame,
    *,
    column: str,
    networks: list[str],
    buses: list[str] | None = None,
    figsize: tuple = (14, 5),
    color_map: dict | None = None,
):
    """
    Matplotlib stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df        : formatted statistics DataFrame (output of ``format_stats_df``).
    column    : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks  : scenario labels to include (one subplot per network).
    buses     : optional bus filter; all buses shown if None.
    color_map : optional dict mapping technology name to hex color. Pass
                ``n.carriers.color.to_dict()`` to use PyPSA carrier colors.
                Falls back to ``_DAMAGE_COLORS`` cycling if None.

    Returns
    -------
    fig, axes : matplotlib Figure and array of Axes
    """
    data = df[df["network"].isin(networks)].copy()
    data = data[data[column] != 0]
    if buses is not None:
        data = data[data["bus"].isin(buses)]

    techs = data["tech"].unique()
    if color_map is None:
        color_map = {t: c for t, c in zip(techs, cycle(_DAMAGE_COLORS))}

    n = len(networks)
    fig, axes = plt.subplots(1, n, figsize=figsize, sharey=True)
    if n == 1:
        axes = [axes]

    for ax, net in zip(axes, networks):
        sub = (
            data[data["network"] == net]
            .groupby(["bus", "tech"])[column]
            .sum()
            .unstack(fill_value=0)
        )
        colors = [color_map.get(c, "#aaaaaa") for c in sub.columns]
        sub.plot(kind="bar", stacked=True, ax=ax, color=colors, legend=(ax is axes[-1]))
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(net)
        ax.set_xlabel("Bus")
        ax.tick_params(axis="x", rotation=30)
        ax.grid(True, which="major", axis="y", alpha=0.3, linestyle="--", linewidth=0.5)
        ax.grid(True, which="major", axis="x", alpha=0.3, linestyle="--", linewidth=0.5)

    axes[0].set_ylabel(column)
    fig.tight_layout()
    return fig, axes


# Carrier color overrides applied before any network map plot.
_CARRIER_COLOR_OVERRIDES = {
    "load_shedding": "darkred",
}


def eb_imap_network(
    n,
    eb: pd.Series,
    bus_label_exclude: list[str] = ("H2",),
    arrow_size_factor: float = 3.0,
    label_size: int = 14,
    **explore_kwargs,
):
    """
    Interactive pydeck map of a solved network.

    Shows energy balance as pie charts per bus, net transmission flows as
    directed arrows, and bus name labels.

    Parameters
    ----------
    n : pypsa.Network
        Solved network (used for coordinates, flows, and carrier colors).
    eb : pd.Series
        Pre-computed ``(bus, carrier)`` MultiIndex Series to display as bus
        sizes.  Pass ``n.statistics.energy_balance(...)`` or
        ``energy_balance_diff`` to show changes relative to a base network.
    bus_label_exclude : list[str]
        Bus names containing any of these strings are not labelled.
    arrow_size_factor : float
        Controls arrowhead size (default 3.0).
    label_size : int
        Font size of bus labels in pixels (default 14).
    **explore_kwargs
        Additional keyword arguments forwarded to ``n.explore()``.

    Returns
    -------
    pydeck.Deck
    """
    # Fill missing/empty carrier colors with lightgrey, then apply overrides
    mask = n.carriers.color.isna() | n.carriers.color.eq("")
    n.carriers.loc[mask, "color"] = "lightgrey"
    for carrier, color in _CARRIER_COLOR_OVERRIDES.items():
        if carrier in n.carriers.index:
            n.carriers.loc[carrier, "color"] = color

    # eb may contain carriers from another network absent from n.carriers
    missing = eb.index.get_level_values("carrier").unique().difference(n.carriers.index)
    for carrier in missing:
        n.carriers.loc[carrier, "color"] = _CARRIER_COLOR_OVERRIDES.get(carrier, "lightgrey")

    line_flow = n.lines_t.p0.sum(axis=0) if not n.lines.empty else pd.Series(dtype=float)
    link_flow = n.links_t.p0.sum(axis=0) if not n.links.empty else pd.Series(dtype=float)

    defaults = dict(
        bus_size=eb,
        bus_split_circle=True,
        line_flow=line_flow,
        link_flow=link_flow,
        line_width=line_flow.abs(),
        link_width=link_flow.abs(),
        auto_scale=True,
        geomap=True,
        geomap_color={"land": "whitesmoke", "ocean": "lightblue"},
        arrow_size_factor=arrow_size_factor,
        tooltip=True,
    )
    defaults.update(explore_kwargs)

    deck = n.explore(**defaults)

    label_data = [
        {"position": [n.buses.x[b], n.buses.y[b]], "label": b}
        for b in n.buses.index
        if not any(ex in b for ex in bus_label_exclude)
    ]
    deck.layers.append(pdk.Layer(
        "TextLayer",
        data=label_data,
        get_position="position",
        get_text="label",
        get_size=label_size,
        get_color=[30, 30, 30, 220],
        get_anchor="'middle'",
        get_alignment_baseline="'bottom'",
    ))

    return deck