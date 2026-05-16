"""
Shared plotting constants and utilities for build_damage_profiles.

Imported by both:
  plot_nuclear_damage_profiles.py     (Plotly / interactive)
  plot_nuclear_damage_profiles_mpl.py (Matplotlib / reports)

Adding a new damage-profile plot type? Import from here so colours and
tick logic stay consistent across all backends.
"""

import colorsys
from itertools import cycle
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).parent

# Nuclear config constants (_DWT, _SWT, _DWT_C, _SWT_C) are loaded lazily on
# first access so that importing this module doesn't require scripts.* to be
# on sys.path (e.g. when only the colour/tick utilities are needed).
_nuclear_cfg: dict = {}


def _ensure_nuclear_cfg() -> None:
    if not _nuclear_cfg:
        from scripts.build_damage_profiles.build_nuclear_damage_profiles import load_damage_config
        cfg = load_damage_config()["nuclear"]
        dwt = cfg["design_water_temp"]
        swt = cfg["shutdown_water_temp"]
        _nuclear_cfg.update(
            _DWT=dwt,
            _SWT=swt,
            _DWT_C=dwt - 273.0,
            _SWT_C=swt - 273.0,
        )


def __getattr__(name: str):
    if name in ("_DWT", "_SWT", "_DWT_C", "_SWT_C"):
        _ensure_nuclear_cfg()
        return _nuclear_cfg[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# ---------------------------------------------------------------------------
# Colour constants — two visually distinct palettes
# ---------------------------------------------------------------------------

# Damage traces: cool palette (blues, purples, pinks, browns, grays).
# Explicitly excludes greens, teals, and all warm tones so that damage lines
# are never confused with the temperature-axis data.
_DAMAGE_COLORS = [
    "#1f77b4",  # steel blue
    "#9467bd",  # muted purple
    "#e377c2",  # raspberry pink
    "#8c564b",  # chestnut brown
    "#7f7f7f",  # middle gray
    "#c5b0d5",  # light lavender
    "#f7b6d2",  # light pink
    "#aec7e8",  # light blue
]
_DAMAGE_SINGLE_COLOR = "#1f77b4"  # single-plant and aggregate lines

# Temperature traces: warm palette only.
_TEMP_LINE_COLOR     = "darkorange"
_TEMP_BAND_FILL_RGBA = "rgba(255, 160, 50, 0.20)"  # Plotly fillcolor string
# Matplotlib fill_between: use _TEMP_LINE_COLOR directly with alpha=0.20

# Threshold lines on the temperature axis.
_DWT_COLOR = "mediumseagreen"
_SWT_COLOR = "crimson"

# _DWT_C and _SWT_C are provided via __getattr__ (lazy-loaded on first access).


# ---------------------------------------------------------------------------
# Backend-agnostic data helpers
# ---------------------------------------------------------------------------

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
    from scripts.build_damage_profiles.build_nuclear_damage_profiles import extract_lake_temp
    raw = extract_lake_temp(cutout_data, plant_row["lat"], plant_row["lon"], time_index)
    return raw - 273.0


# ---------------------------------------------------------------------------
# Tick utility (backend-agnostic)
# ---------------------------------------------------------------------------

def _compute_day_ticks(
    time_index: pd.DatetimeIndex,
) -> tuple[list, list]:
    """
    One major tick per unique calendar day.

      - First occurrence of each month  →  label "MM-DD"
      - All other days                  →  label "DD"

    Returns
    -------
    tickvals : list[pd.Timestamp]   midnight of each day
    ticktext : list[str]
    """
    unique_days = time_index.normalize().unique().sort_values()
    tickvals: list = []
    ticktext: list = []
    seen_months: set = set()

    for day in unique_days:
        tickvals.append(day)
        key = (day.year, day.month)
        if key not in seen_months:
            ticktext.append(day.strftime("%m-%d"))
            seen_months.add(key)
        else:
            ticktext.append(day.strftime("%d"))

    return tickvals, ticktext


# ---------------------------------------------------------------------------
# Colour utilities
# ---------------------------------------------------------------------------

def _darken_hex(hex_color: str, factor: float = 0.55) -> str:
    """Return a darker shade of a hex color by reducing HLS lightness."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) / 255 for i in (0, 2, 4))
    hue, light, sat = colorsys.rgb_to_hls(r, g, b)
    r2, g2, b2 = colorsys.hls_to_rgb(hue, max(0.0, light * factor), sat)
    return f"#{int(r2*255):02x}{int(g2*255):02x}{int(b2*255):02x}"


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a hex color to an ``rgba(...)`` string with the given alpha."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def show_fullscreen(fig) -> None:
    """Open a Plotly figure in a new browser tab."""
    import plotly.io as pio
    pio.show(fig, renderer="browser")


# ---------------------------------------------------------------------------
# Statistics utilities
# ---------------------------------------------------------------------------

def compute_stats_diff(stats_df: pd.DataFrame, base: str = "base") -> pd.DataFrame:
    """
    Append ``{col}_diff`` columns to a statistics DataFrame showing the
    difference of each column relative to a base network.

    Parameters
    ----------
    stats_df : pd.DataFrame
        Output of ``nc.statistics(...)``.
        Must have a MultiIndex containing a level with the base label.
    base : str
        Network label to use as the baseline (default ``"base"``).

    Returns
    -------
    pd.DataFrame
        Original DataFrame plus ``{col}_diff`` columns
        (e.g. ``"Optimal Capacity"`` → ``"optimal_capacity_diff"``).
        Base-network rows have diff = 0; other networks show value − base_value.
        Combinations missing from the base will be NaN.
    """
    level = next(
        (i for i, vals in enumerate(stats_df.index.levels) if base in vals),
        None,
    )
    if level is None:
        raise ValueError(f"Base label {base!r} not found in any index level.")

    base_slice = stats_df.xs(base, level=level)

    # reindex (not loc) so components absent from base get NaN → treated as 0
    aligned_base = base_slice.reindex(stats_df.index.droplevel(level)).fillna(0)
    aligned_base.index = stats_df.index

    diff = stats_df - aligned_base

    result = stats_df.copy()
    for col in stats_df.columns:
        result[col.lower().replace(" ", "_") + "_diff"] = diff[col]

    return result


def format_stats_df(
    df: pd.DataFrame,
    index_names: list[str] = ("techgroup", "network","bus", "tech"),
) -> pd.DataFrame:
    """
    Rename MultiIndex levels and lowercase all column names.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ``nc.statistics(...)`` or ``compute_stats_diff(...)``.
    index_names : list[str]
        Names to assign to the index levels, in order.

    Returns
    -------
    pd.DataFrame
    """
    result = df.copy()
    result.index.names = list(index_names)
    result.columns = [c.lower() for c in result.columns]
    return result.reset_index()


def iplot_stats_bar(
    df: pd.DataFrame,
    column: str,
    networks: list[str],
    buses: list[str] | None = None,
) -> "go.Figure":
    """
    Interactive stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df       : formatted statistics DataFrame (output of ``format_stats_df``).
    column   : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks : scenario labels to include (one facet panel per network).
    buses    : optional bus filter; all buses shown if None.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    import plotly.express as px

    data = df[df["network"].isin(networks)].copy()
    data = data[data[column] != 0]
    if buses is not None:
        data = data[data["bus"].isin(buses)]

    fig = px.bar(
        data,
        x="bus",
        y=column,
        color="tech",
        facet_col="network",
        barmode="relative",
        labels={column: column, "bus": "Bus"},
    )
    fig.update_xaxes(tickangle=30, matches=None)
    fig.add_hline(y=0, line_width=0.8, line_color="black")
    return fig


def plot_stats_bar(
    df: pd.DataFrame,
    column: str,
    networks: list[str],
    buses: list[str] | None = None,
    figsize: tuple = (14, 5),
):
    """
    Matplotlib stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df       : formatted statistics DataFrame (output of ``format_stats_df``).
    column   : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks : scenario labels to include (one subplot per network).
    buses    : optional bus filter; all buses shown if None.

    Returns
    -------
    fig, axes : matplotlib Figure and array of Axes
    """
    data = df[df["network"].isin(networks)].copy()
    data = data[data[column] != 0]
    if buses is not None:
        data = data[data["bus"].isin(buses)]

    techs = data["tech"].unique()
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
    eb: "pd.Series | None" = None,
    components: list[str] = ("Generator", "Load", "StorageUnit"),
    bus_label_exclude: list[str] = ("H2",),
    arrow_size_factor: float = 3.0,
    label_size: int = 14,
    **explore_kwargs,
):
    """
    Interactive pydeck map of a solved network.

    Shows energy balance (or a pre-computed diff) as pie charts per bus,
    net transmission flows as directed arrows, and bus name labels.

    Parameters
    ----------
    n : pypsa.Network
        Solved network (used for coordinates, flows, and carrier colors).
    eb : pd.Series or None
        Pre-computed ``(bus, carrier)`` MultiIndex Series to display as bus
        sizes.  Pass ``energy_balance_diff`` here to show changes relative to
        a base network.  If None, the energy balance is computed from ``n``.
    components : list[str]
        Components included when computing ``eb`` from ``n`` (ignored if
        ``eb`` is supplied).
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
    import pydeck as pdk

    # Fill missing/empty carrier colors with lightgrey, then apply overrides
    mask = n.carriers.color.isna() | n.carriers.color.eq("")
    n.carriers.loc[mask, "color"] = "lightgrey"
    for carrier, color in _CARRIER_COLOR_OVERRIDES.items():
        if carrier in n.carriers.index:
            n.carriers.loc[carrier, "color"] = color

    # If eb is pre-computed (e.g. a diff), it may contain carriers from another
    # network that are absent from n.carriers — add them with defaults/overrides.
    if eb is not None:
        missing = eb.index.get_level_values("carrier").unique().difference(n.carriers.index)
        for carrier in missing:
            n.carriers.loc[carrier, "color"] = _CARRIER_COLOR_OVERRIDES.get(carrier, "lightgrey")

    if eb is None:
        eb = (
            n.statistics.energy_balance(
                groupby=["bus", "carrier"],
                components=list(components),
                nice_names=False,
            )
            .groupby(["bus", "carrier"])
            .sum()
        )

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
