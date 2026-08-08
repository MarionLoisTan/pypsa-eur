import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
from matplotlib.lines import Line2D
from matplotlib.patches import Circle

from builders import compute_stats_diff, format_stats_df

_CARRIER_COLOR_OVERRIDES = {
    "load_shedding": "darkred",
}


def iplot_stats_bar(
    df: pd.DataFrame,
    *,
    column: str,
    networks: list[str],
    buses: list[str] | None = None,
    color_map: dict | None = None,
    title: str | None = None,
    multiplier: float = 1.0,
    aggregate: bool = False,
) -> go.Figure:
    """
    Interactive stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df         : formatted statistics DataFrame (output of ``format_stats_df``).
    column     : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks   : scenario labels to include (one facet panel per network).
    buses      : optional bus filter; all buses shown if None. Ignored when
                 ``aggregate=True``.
    color_map  : optional dict mapping technology name to hex color. Pass
                 ``n.carriers.color.to_dict()`` to use PyPSA carrier colors.
                 Falls back to Plotly defaults if None.
                 ``_CARRIER_COLOR_OVERRIDES`` are always applied on top.
    title      : y-axis label override; defaults to ``column`` name.
    multiplier : scalar applied to ``column`` values before plotting (e.g. 1e-3
                 to convert MW → GW).
    aggregate  : if True, sum across all buses so each panel shows a single
                 stacked bar for the whole system.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    data = df[df["network"].isin(networks)].copy()
    if multiplier != 1.0:
        data[column] = data[column] * multiplier
    data = data[data[column] != 0]
    if not aggregate and buses is not None:
        data = data[data["bus"].isin(buses)]

    effective_map = {**(color_map or {}), **_CARRIER_COLOR_OVERRIDES}
    y_label = title if title is not None else column

    if aggregate:
        data["_x"] = "Total"
        x_col, x_label = "_x", "Total"
    else:
        x_col, x_label = "bus", "Bus"

    fig = px.bar(
        data,
        x=x_col,
        y=column,
        color="tech",
        facet_col="network",
        barmode="relative",
        labels={column: y_label, x_col: x_label},
        color_discrete_map=effective_map,
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
    title: str | None = None,
    multiplier: float = 1.0,
    carrier_multipliers: dict | None = None,
    show_legend: bool = True,
    aggregate: bool = False,
):
    """
    Matplotlib stacked bar chart of any statistics column, grouped by bus.

    Parameters
    ----------
    df          : formatted statistics DataFrame (output of ``format_stats_df``).
    column      : column to plot, e.g. ``"energy_balance_diff"`` or ``"supply"``.
    networks    : scenario labels to include (one subplot per network).
    buses       : optional bus filter; all buses shown if None. Ignored when
                  ``aggregate=True``.
    color_map   : optional dict mapping technology name to hex color. Pass
                  ``n.carriers.color.to_dict()`` to use PyPSA carrier colors.
                  Falls back to grey (#aaaaaa) if None.
                  ``_CARRIER_COLOR_OVERRIDES`` are always applied on top.
    title       : y-axis label override; defaults to ``column`` name.
    multiplier  : scalar applied to ``column`` values before plotting (e.g. 1e-3
                  to convert MW → GW).
    show_legend : whether to draw a shared legend below the subplots.
                  Uses up to 5 columns; wraps to additional rows if needed.
    aggregate   : if True, sum across all buses so each subplot shows a single
                  stacked bar for the whole system.

    Returns
    -------
    fig, axes : matplotlib Figure and array of Axes
    """
    data = df[df["network"].isin(networks)].copy()
    if multiplier != 1.0:
        data[column] = data[column] * multiplier
    if carrier_multipliers:
        for carrier, factor in carrier_multipliers.items():
            mask = data["tech"] == carrier
            data.loc[mask, column] = data.loc[mask, column] * factor
    data = data[data[column] != 0]
    if not aggregate and buses is not None:
        data = data[data["bus"].isin(buses)]

    effective_map = {**(color_map or {}), **_CARRIER_COLOR_OVERRIDES}

    n = len(networks)
    fig, axes = plt.subplots(1, n, figsize=figsize, sharey=True)
    if n == 1:
        axes = [axes]

    for ax, net in zip(axes, networks):
        net_data = data[data["network"] == net]
        if aggregate:
            sub = (
                net_data.groupby("tech")[column]
                .sum()
                .to_frame()
                .T
            )
            sub.index = ["Total"]
        else:
            sub = (
                net_data.groupby(["bus", "tech"])[column]
                .sum()
                .unstack(fill_value=0)
            )
        colors = [effective_map.get(c, "#aaaaaa") for c in sub.columns]
        sub.plot(kind="bar", stacked=True, ax=ax, color=colors, legend=False)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(net)
        ax.set_xlabel("" if aggregate else "Bus")
        ax.tick_params(axis="x", rotation=0 if aggregate else 30)
        ax.grid(True, which="major", axis="y", alpha=0.3, linestyle="--", linewidth=0.5)
        ax.grid(True, which="major", axis="x", alpha=0.3, linestyle="--", linewidth=0.5)

    axes[0].set_ylabel(title if title is not None else column)

    if show_legend:
        all_techs = data["tech"].unique()
        handles = [
            mpatches.Patch(facecolor=effective_map.get(t, "#aaaaaa"), label=t)
            for t in all_techs
        ]
        ncols = min(len(handles), 5)
        fig.tight_layout()
        fig.canvas.draw()
        label_bottoms = [
            ax.xaxis.label.get_window_extent()
            .transformed(fig.transFigure.inverted()).y0
            for ax in axes
        ]
        label_bottom = min(label_bottoms)
        fig.legend(
            handles=handles,
            loc="upper center",
            bbox_to_anchor=(0.5, label_bottom - 0.02),
            bbox_transform=fig.transFigure,
            ncol=ncols,
            frameon=False,
        )
    else:
        fig.tight_layout()

    return fig, axes




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


def eb_plot_network(
    n,
    eb: pd.Series,
    bus_label_exclude: list[str] = ("H2",),
    label_size: int = 10,
    figsize: tuple = (10, 8),
    geomap: bool = False,
    ax=None,
    **plot_kwargs,
):
    """
    Static matplotlib map of a solved network.

    Shows energy balance as split-circle pie charts per bus, net transmission
    flows as directed arrows, and bus name labels. Mirrors ``eb_imap_network``
    but renders via ``n.plot()`` instead of pydeck.

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
    label_size : int
        Font size of bus labels (default 10).
    figsize : tuple
        Figure size passed to ``plt.subplots`` when ``ax`` is None.
    geomap : bool
        Whether to draw a cartopy geographic basemap (requires cartopy).
    ax : matplotlib.axes.Axes or None
        Axes to draw on; created if None.
    **plot_kwargs
        Additional keyword arguments forwarded to ``n.plot()``.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    # Fill missing/empty carrier colors with lightgrey, then apply overrides
    mask = n.carriers.color.isna() | n.carriers.color.eq("")
    n.carriers.loc[mask, "color"] = "lightgrey"
    for carrier, color in _CARRIER_COLOR_OVERRIDES.items():
        if carrier in n.carriers.index:
            n.carriers.loc[carrier, "color"] = color

    missing = eb.index.get_level_values("carrier").unique().difference(n.carriers.index)
    for carrier in missing:
        n.carriers.loc[carrier, "color"] = _CARRIER_COLOR_OVERRIDES.get(carrier, "lightgrey")

    line_flow = n.lines_t.p0.sum(axis=0) if not n.lines.empty else pd.Series(dtype=float)
    link_flow = n.links_t.p0.sum(axis=0) if not n.links.empty else pd.Series(dtype=float)

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    defaults = dict(
        bus_size=eb,
        bus_split_circle=True,
        line_flow=line_flow,
        link_flow=link_flow,
        line_width=line_flow.abs(),
        link_width=link_flow.abs(),
        geomap=geomap,
    )
    defaults.update(plot_kwargs)

    n.plot(ax=ax, **defaults)

    for b in n.buses.index:
        if not any(ex in b for ex in bus_label_exclude):
            ax.annotate(
                b,
                xy=(n.buses.x[b], n.buses.y[b]),
                fontsize=label_size,
                ha="center", va="bottom",
                color="#1e1e1e",
            )

    return fig, ax

def plot_network_map(
    n,
    regions,
    *,
    stat="installed_capacity",
    stat_kwargs=None,
    bus_carrier="AC",
    bus_area_fraction=0.05,
    branch_area_fraction=0.07,
    max_line_width=0.1,
    legend_circles=None,
    legend_lines=None,
    legend_unit="GW",
    legend_multiplier=1e-3,
    legend_spacing=4,
    legend_handletextpad=2,
    legend_bbox=(1.0, 0.0),
    pad=2.0,
    figsize=(8, 8),
    title="",
    ax=None,
    snapshots=None,
):
    """Plot a PyPSA network statistic on a geographic map.

    Parameters
    ----------
    n : pypsa.Network
    regions : geopandas.GeoDataFrame
        Bus region boundaries to overlay.
    stat : str
        Name of the statistics function to plot: "installed_capacity",
        "energy_balance", "supply", etc.
    stat_kwargs : dict, optional
        Passed to the statistics function via `stats_kwargs` (e.g. snapshot
        filters for energy_balance).
    bus_carrier : str
        Bus carrier to filter on (default "AC").
    bus_area_fraction : float
        Fraction of map area occupied by bus pies.
    branch_area_fraction : float
        Fraction of map area occupied by branch width patches.
    max_line_width : float
        Maximum line/link width in data coordinates.
    legend_circles : list of float, optional
        Representative values for the circle legend, in the same units as the
        plotted statistic (MW for installed_capacity, MWh for energy_balance).
    legend_lines : list of float, optional
        Representative line capacity values (MW) for the line width legend.
    legend_unit : str
        Unit label shown next to each legend entry (default "GW").
    legend_multiplier : float
        Scalar applied to legend values before display (default 1e-3 for MW→GW).
        Use 1e-6 for MWh→TWh, etc.
    legend_spacing : float
        `labelspacing` passed to `ax.legend()` (default 4).
    pad : float
        Extra padding added around the network extent (data units).
    figsize : tuple
        Figure size in inches.
    title : str
        Plot title.
    ax : matplotlib.axes.Axes, optional
        Existing axes to draw on. Creates a new figure if None.

    Returns
    -------
    fig, ax
    """
    from pypsa.geo import compute_bbox
    from pypsa.plot.maps.static import HandlerCircle

    if title:
        print(title)
    stat_kwargs = stat_kwargs or {}

    # Build a snapshot-filtered copy for statistics (set_snapshots is destructive,
    # so we copy to avoid modifying the caller's network object).
    if snapshots is not None:
        if isinstance(snapshots, slice):
            mask = pd.Series(True, index=n.snapshots)
            if snapshots.start is not None:
                mask &= n.snapshots >= pd.Timestamp(snapshots.start)
            if snapshots.stop is not None:
                mask &= n.snapshots <= pd.Timestamp(snapshots.stop)
            snap_idx = n.snapshots[mask.values]
        else:
            snap_idx = pd.DatetimeIndex(snapshots)
        n_plot = n.copy()
        n_plot.set_snapshots(snap_idx)
    else:
        n_plot = n

    def _visible_widths(df, nom_col):
        """Return per-branch width Series for branches with non-zero visual length."""
        if df.empty:
            return None
        lengths = np.hypot(
            df.bus1.map(n.buses.x) - df.bus0.map(n.buses.x),
            df.bus1.map(n.buses.y) - df.bus0.map(n.buses.y),
        )
        visible = df[lengths > 0]
        if visible.empty:
            return None
        return (visible[nom_col] / visible[nom_col].max() * max_line_width).clip(lower=0.01)

    line_widths = _visible_widths(n.lines, "s_nom")
    # Only draw DC transmission links as branches. PyPSA colours ALL links in a
    # branch_component using the stat (energy_balance), and non-transmission links
    # (H2 electrolysis, fuel cells) return NaN colours under bus_carrier="AC",
    # which crashes PatchCollection. We therefore only include "Link" in
    # branch_components when every link is a DC transmission link.
    dc_links = n.links[n.links.carrier == "DC"]
    has_non_dc_links = len(dc_links) < len(n.links)
    dc_widths = _visible_widths(dc_links, "p_nom")
    link_widths = dc_widths if (dc_widths is not None and not has_non_dc_links) else None

    # Only render components that have at least one visible (non-zero length) branch.
    # Passing branch_components prevents PyPSA from trying to draw zero-length
    # branches, which would crash with an empty PatchCollection.
    branch_components = (
        (["Line"] if line_widths is not None else []) +
        (["Link"] if link_widths is not None else [])
    )

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    # Fix carriers with empty-string or NaN colors
    carrier_colors = n.carriers.color.fillna("").map(lambda c: c if c else "#aaaaaa")

    # Build a shared norm across all branch components (Line + Link combined).
    # PyPSA normalises each component independently, which collapses to NaN when
    # only one branch of a type exists (vmin == vmax → 0/0). Using a shared norm
    # avoids this — the single Line is coloured relative to the full branch range.
    _eb_all = getattr(n_plot.statistics, stat)(nice_names=False, **stat_kwargs)
    _branch_vals = _eb_all[
        (_eb_all.index.get_level_values("component") == "Line") |
        (
            (_eb_all.index.get_level_values("component") == "Link") &
            (_eb_all.index.get_level_values("carrier") == "DC")
        )
    ]
    if not _branch_vals.empty:
        _bmin, _bmax = _branch_vals.min(), _branch_vals.max()
        if _bmin == _bmax:
            _bmin -= abs(_bmin) or 1
            _bmax += abs(_bmax) or 1
        _branch_norm = plt.Normalize(vmin=_bmin, vmax=_bmax)
    else:
        _branch_norm = None

    getattr(n_plot.statistics, stat).plot.map(
        bus_carrier=bus_carrier,
        title="",
        bus_area_fraction=bus_area_fraction,
        branch_area_fraction=branch_area_fraction,
        draw_legend_circles=False,
        draw_legend_lines=False,
        draw_legend_arrows=False,
        draw_legend_patches=True,
        geomap=False,
        line_width=line_widths if line_widths is not None else 0.0,
        link_width=link_widths if link_widths is not None else 0.0,
        branch_components=branch_components,
        bus_color=carrier_colors,
        ax=ax,
        **({
            "line_cmap_norm": _branch_norm,
            "link_cmap_norm": _branch_norm,
        } if _branch_norm is not None else {}),
    )

    regions.boundary.plot(ax=ax, color="steelblue", linewidth=1.0, alpha=0.7)

    ax.set_aspect("equal")
    xl, yl = ax.get_xlim(), ax.get_ylim()
    ax.set_xlim(xl[0] - pad, xl[1] + pad)
    ax.set_ylim(yl[0] - pad, yl[1] + pad)

    # ── Bus labels ────────────────────────────────────────────────────────────
    buses = n.buses[n.buses.carrier == bus_carrier]
    for bus, row in buses.iterrows():
        ax.annotate(
            bus,
            xy=(row.x, row.y),
            xytext=(12, 10), textcoords="offset points",
            fontsize=10, ha="left", va="bottom",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="black", linewidth=0.8),
            zorder=10,
        )

    # ── Legend ────────────────────────────────────────────────────────────────
    if legend_circles is not None or legend_lines is not None:
        # Replicate PyPSA's internal scaling (bbox from all buses, 5% margin)
        bus_size_mc = getattr(n_plot.statistics, stat)(
            bus_carrier=bus_carrier, groupby=["bus", "carrier"], nice_names=False,
            **stat_kwargs,
        )
        (x1, y1), (x2, y2) = compute_bbox(n.buses.x, n.buses.y, margin=0.05)
        scaling_factor = bus_area_fraction * (x2 - x1) * (y2 - y1) / bus_size_mc.abs().sum()

        fig.canvas.draw()
        pts_per_data = (
            ax.get_window_extent().width / fig.dpi * 72
            / (ax.get_xlim()[1] - ax.get_xlim()[0])
        )

        handles, labels = [], []

        if legend_circles is not None:
            for v in legend_circles:
                handles.append(Circle((0, 0), radius=(v * scaling_factor) ** 0.5))
                labels.append(f"{v * legend_multiplier:.3g} {legend_unit}")

        if legend_lines is not None and not n.lines.empty:
            for v in legend_lines:
                lw = v / n.lines.s_nom.max() * max_line_width * pts_per_data
                handles.append(Line2D([0], [0], linewidth=lw, color="gray"))
                labels.append(f"{v * legend_multiplier:.3g} {legend_unit}")

        leg = ax.legend(
            handles, labels,
            handler_map={Circle: HandlerCircle()},
            bbox_to_anchor=legend_bbox,
            loc="upper left", frameon=False, fontsize=10,
            labelspacing=legend_spacing, handletextpad=legend_handletextpad,
        )
        fig.add_artist(leg)

    return fig, ax
