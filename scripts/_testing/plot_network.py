"""
Capacity factor and p_max_pu plotting helpers for network inspection notebooks.

Builders live in builders.py — this file contains only rendering functions.

Public API
----------
plot_p_max_pu          Matplotlib line plot  (build_p_max_pu_df → here)
iplot_p_max_pu         Interactive Plotly    (build_p_max_pu_df → here)
plot_cf_pmax           Matplotlib CF vs p_max_pu comparison  (build_cf_pmax_df → here)
iplot_cf_pmax          Interactive Plotly CF vs p_max_pu     (build_cf_pmax_df → here)
plot_monthly_cf_pmax   Matplotlib grouped bar (monthly CF) + dot (p_max_pu) by scenario
iplot_monthly_cf_pmax  Interactive Plotly equivalent
"""

from itertools import cycle

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go

from builders import build_p_max_pu_df, build_cf_pmax_df
from styles import (
    _DAMAGE_COLORS,
    _compute_day_ticks,
    _darken_hex,
    _hex_to_rgba,
    show_fullscreen,
)


# ---------------------------------------------------------------------------
# Matplotlib
# ---------------------------------------------------------------------------

def plot_p_max_pu(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (8, 4),
    ax: plt.Axes | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """
    Matplotlib line plot of p_max_pu.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_p_max_pu_df``.
        Index: DatetimeIndex. Columns: ``"{label} | {carrier}"`` etc.
    title : str or None
    figsize : tuple
    ax : plt.Axes or None
        Pass to draw into existing axes.

    Returns
    -------
    fig, ax
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    color_cycle = cycle(_DAMAGE_COLORS)
    for col in data.columns:
        ax.plot(data.index, data[col], label=col, color=next(color_cycle), linewidth=1.5)

    tickvals, ticktext = _compute_day_ticks(data.index)
    ax.set_xticks(tickvals)
    ax.set_xticklabels(ticktext, rotation=30, ha="right", fontsize=8)
    ax.xaxis.set_minor_locator(plt.matplotlib.dates.HourLocator(byhour=[6, 12, 18]))

    ax.set_ylim(0, 1)
    ax.grid(True, which="major", axis="both", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.set_ylabel("p_max_pu")
    ax.set_xlabel("snapshot")
    if title is not None:
        ax.set_title(title)
    ax.legend(loc="upper right", fontsize=8, framealpha=0.8)
    fig.tight_layout()
    return fig, ax


# ---------------------------------------------------------------------------
# Plotly
# ---------------------------------------------------------------------------

def iplot_p_max_pu(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,
    open_in_browser: bool = False,
) -> go.Figure:
    """
    Interactive Plotly line plot of p_max_pu.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_p_max_pu_df``.
    title : str or None
    fig : go.Figure or None
        Pass to add traces to an existing figure.
    open_in_browser : bool

    Returns
    -------
    go.Figure
    """
    if fig is None:
        fig = go.Figure()

    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    scenario_colors = {
        label: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)] for i, label in enumerate(scenario_labels)
    }

    for col in data.columns:
        label = col.split(" | ")[0]
        fig.add_trace(go.Scatter(
            x=data.index, y=data[col], name=col,
            mode="lines",
            line=dict(color=scenario_colors[label], width=1.5),
            connectgaps=False,
        ))

    fig.update_layout(
        hovermode="x unified",
        title=title,
        yaxis=dict(range=[0, 1], title="p_max_pu"),
        xaxis_title="snapshot",
    )
    if open_in_browser:
        show_fullscreen(fig)
    return fig


# ---------------------------------------------------------------------------
# CF vs p_max_pu comparison
# ---------------------------------------------------------------------------

def plot_cf_pmax(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (8, 4),
    ax: plt.Axes | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """
    Matplotlib comparison of capacity_factor (solid fill) vs p_max_pu (dotted).

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_cf_pmax_df``.
        Index: DatetimeIndex. Columns: ``"{label} | CF"`` and ``"{label} | p_max_pu"``.
    title : str or None
    figsize : tuple
    ax : plt.Axes or None

    Returns
    -------
    fig, ax
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    color_cycle = cycle(_DAMAGE_COLORS)
    scenario_colors = {label: next(color_cycle) for label in scenario_labels}

    for col in data.columns:
        label = col.split(" | ")[0]
        is_cf = col.endswith("| CF")
        color = scenario_colors[label]
        if is_cf:
            ax.fill_between(data.index, data[col], alpha=0.25, color=color)
            ax.plot(data.index, data[col], color=color, linewidth=0.5, label=col)
        else:
            ax.plot(data.index, data[col], color=_darken_hex(color), linestyle=":", linewidth=1.5, label=col)

    tickvals, ticktext = _compute_day_ticks(data.index)
    ax.set_xticks(tickvals)
    ax.set_xticklabels(ticktext, rotation=90, ha="center", fontsize=8)
    ax.xaxis.set_minor_locator(plt.matplotlib.dates.HourLocator(byhour=[6, 12, 18]))

    ax.set_ylim(0, 1)
    ax.grid(True, which="major", axis="both", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.set_ylabel("p.u.")
    if title is not None:
        print(title)
    ax.legend(loc="upper right", fontsize=8, framealpha=0.8)
    fig.tight_layout()
    return fig, ax


def iplot_cf_pmax(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,
    open_in_browser: bool = False,
) -> go.Figure:
    """
    Interactive Plotly comparison of capacity_factor (filled) vs p_max_pu (dotted).

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_cf_pmax_df``.
    title : str or None
    fig : go.Figure or None
    open_in_browser : bool

    Returns
    -------
    go.Figure
    """
    if fig is None:
        fig = go.Figure()

    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    scenario_colors = {
        label: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)] for i, label in enumerate(scenario_labels)
    }

    for col in data.columns:
        label = col.split(" | ")[0]
        is_cf = col.endswith("| CF")
        color = scenario_colors[label]
        if is_cf:
            fig.add_trace(go.Scatter(
                x=data.index, y=data[col], name=col,
                mode="lines",
                fill="tozeroy",
                fillcolor=_hex_to_rgba(color, 0.2),
                line=dict(color=color, width=1),
                connectgaps=False,
            ))
        else:
            fig.add_trace(go.Scatter(
                x=data.index, y=data[col], name=col,
                mode="lines",
                line=dict(color=_darken_hex(color), width=1.5, dash="dot"),
                connectgaps=False,
            ))

    fig.update_layout(
        hovermode="x unified",
        title=title,
        yaxis=dict(range=[0, 1], title="p.u."),
        xaxis_title="snapshot",
    )
    if open_in_browser:
        show_fullscreen(fig)
    return fig


# ---------------------------------------------------------------------------
# Monthly CF vs p_max_pu — grouped by scenario and month
# ---------------------------------------------------------------------------

def _tick_labels(index: pd.DatetimeIndex, freq: str) -> list[str]:
    if freq == "total":
        return ["Total"]
    if freq in ("W", "W-MON", "W-SUN"):
        return [t.strftime("W%V %Y") for t in index]
    if freq == "MS":
        return [t.strftime("%b %Y") for t in index]
    return [str(t.date()) for t in index]


def plot_agg_cf_pmax(
    data: pd.DataFrame,
    *,
    freq: str = "MS",
    title: str | None = None,
    figsize: tuple = (12, 5),
    ax: plt.Axes | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """
    Grouped bar chart of aggregated CF with p_max_pu horizontal-line overlay.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_monthly_cf_pmax_df``.
        Columns: ``"{label} | CF"`` and ``"{label} | p_max_pu"``.
    freq : str
        Resample frequency used when building ``data`` — controls tick labels.
        ``"MS"`` monthly, ``"W"`` weekly, ``"total"`` single bar.
    title : str or None
    figsize : tuple
    ax : plt.Axes or None

    Returns
    -------
    fig, ax
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    n_scenarios = len(scenario_labels)
    n_months = len(data)
    bar_width = 0.7 / n_scenarios
    offsets = np.linspace(
        -(n_scenarios - 1) / 2 * bar_width,
         (n_scenarios - 1) / 2 * bar_width,
        n_scenarios,
    )
    x = np.arange(n_months)

    for i, label in enumerate(scenario_labels):
        color = _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
        xi = x + offsets[i]
        cf_col = f"{label} | CF"
        pmax_col = f"{label} | p_max_pu"

        if cf_col in data.columns:
            ax.bar(xi, data[cf_col], width=bar_width, color=color,
                   alpha=0.75, label=f"{label} CF")
        if pmax_col in data.columns:
            ax.plot(xi, data[pmax_col], "_",
                    color=_darken_hex(color), markersize=10, zorder=5,
                    label=f"{label} p_max_pu",
                    markeredgewidth=2)

    tick_lbls = _tick_labels(data.index, freq)
    ax.set_xticks(x)
    ax.set_xticklabels(tick_lbls, rotation=30, ha="right")
    ax.set_ylim(0, 1)
    ax.grid(True, axis="y", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.set_ylabel("p.u.")
    if title is not None:
        print(title)
    ax.legend(loc="upper right", framealpha=0.8)
    fig.tight_layout()
    return fig, ax


def iplot_agg_cf_pmax(
    data: pd.DataFrame,
    *,
    freq: str = "MS",
    title: str | None = None,
    open_in_browser: bool = False,
) -> go.Figure:
    """
    Interactive Plotly grouped bar chart of aggregated CF with p_max_pu overlay.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_monthly_cf_pmax_df``.
        Columns: ``"{label} | CF"`` and ``"{label} | p_max_pu"``.
    freq : str
        Resample frequency used when building ``data`` — controls tick labels.
        ``"MS"`` monthly, ``"W"`` weekly, ``"total"`` single bar.
    title : str or None
    open_in_browser : bool

    Returns
    -------
    go.Figure
    """
    scenario_labels = sorted({col.split(" | ")[0] for col in data.columns})
    n_scenarios = len(scenario_labels)
    n_bins = len(data)
    bar_width = 0.7 / n_scenarios
    offsets = np.linspace(
        -(n_scenarios - 1) / 2 * bar_width,
         (n_scenarios - 1) / 2 * bar_width,
        n_scenarios,
    )
    x = np.arange(n_bins)
    month_labels = _tick_labels(data.index, freq)

    scenario_colors = {
        label: _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
        for i, label in enumerate(scenario_labels)
    }

    fig = go.Figure()
    for i, label in enumerate(scenario_labels):
        color = scenario_colors[label]
        xi = (x + offsets[i]).tolist()
        cf_col = f"{label} | CF"
        pmax_col = f"{label} | p_max_pu"

        if cf_col in data.columns:
            fig.add_trace(go.Bar(
                x=xi, y=data[cf_col].tolist(),
                name=f"{label} CF",
                marker_color=color,
                opacity=0.75,
                width=bar_width,
                customdata=month_labels,
                hovertemplate="%{customdata}<br>CF: %{y:.3f}<extra>" + label + "</extra>",
            ))
        if pmax_col in data.columns:
            fig.add_trace(go.Scatter(
                x=xi, y=data[pmax_col].tolist(),
                name=f"{label} p_max_pu",
                mode="markers",
                marker=dict(
                    color=_darken_hex(color),
                    size=12,
                    symbol="line-ew",
                    line=dict(color=_darken_hex(color), width=2),
                ),
                customdata=month_labels,
                hovertemplate="%{customdata}<br>p_max_pu: %{y:.3f}<extra>" + label + "</extra>",
            ))

    fig.update_layout(
        title=title,
        hovermode="x unified",
        yaxis=dict(range=[0, 1], title="p.u."),
        xaxis=dict(tickmode="array", tickvals=x.tolist(), ticktext=month_labels),
    )
    if open_in_browser:
        show_fullscreen(fig)
    return fig
