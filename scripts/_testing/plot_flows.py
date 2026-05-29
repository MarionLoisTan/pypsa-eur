"""
Bus-level net AC flow plotting helpers.

Builders live in builders.py — this file contains only rendering functions.

Public API
----------
plot_bus_net_flow    Matplotlib subplots (one panel per bus)
iplot_bus_net_flow   Interactive Plotly figure

Sign convention: positive = net exporting, negative = net importing.
Only AC lines (n.lines) are used — links in this network are H2 conversion
assets, not inter-bus AC transmission.
"""

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go

from styles import _DAMAGE_COLORS, _compute_day_ticks
from builders import build_bus_net_flow_df

_DASH_STYLES_PLOTLY = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]
_DASH_STYLES_MPL    = ["solid", "dashed", "dotted", "dashdot"]


# ---------------------------------------------------------------------------
# Matplotlib
# ---------------------------------------------------------------------------

def plot_bus_net_flow(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (14, 4),
) -> tuple[plt.Figure, list]:
    """
    Matplotlib line plot of net AC export per bus.

    One subplot per bus; scenarios are overlaid within each subplot using
    distinct colours from ``_DAMAGE_COLORS``.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_bus_net_flow_df``.
        Index: DatetimeIndex. Columns: ``"{label} | {bus}"``.
    title : str or None
    figsize : tuple

    Returns
    -------
    fig, axes
    """
    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    all_buses = sorted({col.split(" | ")[1] for col in data.columns})
    color_map = {
        lbl: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
        for i, lbl in enumerate(scenario_labels)
    }

    n_buses = len(all_buses)
    fig, axes = plt.subplots(1, n_buses, figsize=figsize, sharey=True)
    if n_buses == 1:
        axes = [axes]

    tickvals, ticktext = _compute_day_ticks(data.index)

    for ax, bus in zip(axes, all_buses):
        ax.axhline(0, color="black", linewidth=0.6, linestyle="--", zorder=1)
        for lbl in scenario_labels:
            col = f"{lbl} | {bus}"
            if col not in data.columns:
                continue
            ax.plot(
                data.index, data[col],
                label=lbl,
                color=color_map[lbl],
                linewidth=1.0,
                zorder=2,
            )
        ax.set_title(bus, fontsize=9)
        ax.set_xticks(tickvals)
        ax.set_xticklabels(ticktext, rotation=30, ha="right", fontsize=7)
        ax.xaxis.set_minor_locator(plt.matplotlib.dates.HourLocator(byhour=[6, 12, 18]))
        ax.grid(True, which="major", alpha=0.3, linestyle="--", linewidth=0.5)

    axes[0].set_ylabel("Net export (MW)")
    handles, labels_ = axes[-1].get_legend_handles_labels()
    fig.legend(handles, labels_, loc="upper right", fontsize=8, framealpha=0.8)
    fig.suptitle(
        title or "Net AC export per bus  (+ = exporting, − = importing)",
        fontsize=10,
    )
    fig.tight_layout()
    return fig, axes


# ---------------------------------------------------------------------------
# Plotly
# ---------------------------------------------------------------------------

def iplot_bus_net_flow(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,
) -> go.Figure:
    """
    Interactive Plotly line plot of net AC export per bus.

    Colour encodes scenario; line dash encodes bus.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_bus_net_flow_df``.
    title : str or None
    fig : go.Figure or None
        Pass to add traces to an existing figure.

    Returns
    -------
    go.Figure
    """
    if fig is None:
        fig = go.Figure()

    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    all_buses = sorted({col.split(" | ")[1] for col in data.columns})

    color_map = {
        lbl: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
        for i, lbl in enumerate(scenario_labels)
    }
    dash_map = {
        bus: _DASH_STYLES_PLOTLY[i % len(_DASH_STYLES_PLOTLY)]
        for i, bus in enumerate(all_buses)
    }

    fig.add_hline(y=0, line_width=0.8, line_color="black", line_dash="dot")

    for col in data.columns:
        lbl, bus = col.split(" | ", 1)
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data[col],
            name=col,
            mode="lines",
            line=dict(color=color_map[lbl], dash=dash_map[bus], width=1.5),
            connectgaps=False,
        ))

    fig.update_layout(
        title=title or "Net AC export per bus  (positive = exporting, negative = importing)",
        yaxis_title="Net export (MW)",
        xaxis_title="Snapshot",
        hovermode="x unified",
        legend=dict(x=1.01, y=1),
    )
    return fig
