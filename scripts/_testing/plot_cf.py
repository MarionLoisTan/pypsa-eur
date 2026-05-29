"""
CF time-series comparison and CF-vs-temperature scatter plots.

Builders live in builders.py — this file contains only rendering functions.

Public API
----------
iplot_cf_comparison   interactive Plotly time-series (build_plant_cf_df → here)
plot_cf_comparison    Matplotlib time-series
iplot_cf_temp         interactive scatter of CF vs lake temp (build_cf_temp_aligned_df → here)
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import plotly.colors
import plotly.graph_objects as go

from builders import build_cf_sources, build_multi_scenario_sources

_PLOTLY_COLORS = plotly.colors.qualitative.Plotly
_SOURCE_COLORS = {"Actual CF": "steelblue", "Damage-adjusted CF": "tomato"}
_SCATTER_SYMBOLS = {"Actual CF": "circle", "Damage-adjusted CF": "x"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_cf_col(col: str) -> tuple[str, int | None]:
    """Parse 'Source YYYY' → (source, year) or (col, None)."""
    parts = col.rsplit(" ", 1)
    if len(parts) == 2:
        try:
            return parts[0], int(parts[1])
        except ValueError:
            pass
    return col, None


def _col_style(col: str, year_order: list[int]) -> dict:
    """Line color and dash for a CF comparison column."""
    source, year = _parse_cf_col(col)
    if year is not None:
        color = _PLOTLY_COLORS[year_order.index(year) % len(_PLOTLY_COLORS)]
    else:
        color = next((v for k, v in _SOURCE_COLORS.items() if col.startswith(k)), "steelblue")
    dash = "dot" if "Damage" in col else "solid"
    return {"color": color, "dash": dash}


def _scatter_style(label: str, year_order: list[int]) -> dict:
    """Marker color and symbol for a CF-vs-temp trace; label uses ' – ' format."""
    if " – " in label:
        year_str, _, source_type = label.partition(" – ")
        year = int(year_str)
        color = _PLOTLY_COLORS[year_order.index(year) % len(_PLOTLY_COLORS)]
        symbol = _SCATTER_SYMBOLS.get(source_type, "circle")
    else:
        color = next((v for k, v in _SOURCE_COLORS.items() if label.startswith(k)), "grey")
        symbol = _SCATTER_SYMBOLS.get(label, "circle")
    return {"color": color, "symbol": symbol}


# ---------------------------------------------------------------------------
# CF time-series
# ---------------------------------------------------------------------------

def iplot_cf_comparison(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,
) -> go.Figure:
    """
    Interactive Plotly time-series CF comparison.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_plant_cf_df``.
        Index: DatetimeIndex. Columns: e.g. ``"Actual CF 2018"``.
    title : str or None
    fig : go.Figure or None
        Pass to add traces to an existing figure.

    Returns
    -------
    go.Figure
    """
    if fig is None:
        fig = go.Figure()

    years = sorted(
        y for col in data.columns
        if (y := _parse_cf_col(col)[1]) is not None
    )

    for col in data.columns:
        style = _col_style(col, years)
        fig.add_trace(go.Scatter(
            x=data.index, y=data[col],
            name=col,
            line=dict(color=style["color"], dash=style["dash"]),
            connectgaps=False,
        ))

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis=dict(title="Capacity factor", range=[0, 1.05]),
        legend=dict(x=1.01, y=1, xanchor="left"),
    )
    return fig


def plot_cf_comparison(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (12, 4),
    ax: plt.Axes | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """
    Matplotlib time-series CF comparison.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_plant_cf_df``.
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

    years = sorted(
        y for col in data.columns
        if (y := _parse_cf_col(col)[1]) is not None
    )

    for col in data.columns:
        style = _col_style(col, years)
        ls = ":" if style["dash"] == "dot" else "-"
        ax.plot(data.index, data[col], color=style["color"], linestyle=ls,
                label=col, linewidth=0.8)

    if title is not None:
        ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Capacity factor")
    ax.set_ylim(0, 1.05)
    ax.legend(loc="upper left", fontsize=8)

    # Overlay mode: all timestamps are in year 2000 — show MM-DD on x-axis
    if not data.empty and set(data.index.year) == {2000}:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))

    fig.autofmt_xdate()
    fig.tight_layout()
    return fig, ax


# ---------------------------------------------------------------------------
# CF vs temperature scatter
# ---------------------------------------------------------------------------

def iplot_cf_temp(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,
) -> go.Figure:
    """
    Interactive scatter of CF vs lake surface temperature.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_cf_temp_aligned_df``.
        Columns: ``plant``, ``label``, ``temp``, ``cf``.
    title : str or None
    fig : go.Figure or None
        Pass to add traces to an existing figure.

    Returns
    -------
    go.Figure
    """
    if fig is None:
        fig = go.Figure()

    year_order = sorted(
        int(lbl.split(" – ")[0])
        for lbl in data["label"].unique()
        if " – " in lbl
    )
    multi_plant = data["plant"].nunique() > 1

    for (plant, label), group in data.groupby(["plant", "label"]):
        style = _scatter_style(label, year_order)
        trace_name = label if not multi_plant else f"{plant} — {label}"
        fig.add_trace(go.Scatter(
            x=group["temp"], y=group["cf"],
            mode="markers",
            marker=dict(symbol=style["symbol"], color=style["color"], size=5, opacity=0.6),
            name=trace_name,
        ))

    fig.update_layout(
        title=title or "CF vs lake surface temperature",
        xaxis_title="Lake surface temperature (K)",
        yaxis=dict(title="Capacity factor", range=[0, 1.05]),
        legend=dict(x=1.01, y=1),
    )
    return fig
