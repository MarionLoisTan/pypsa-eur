"""
Nuclear damage profile plot functions — Plotly (iplot_*) and Matplotlib (plot_*).

Builders live in builders.py — this file contains only rendering functions.

Public API
----------
iplot_plant_profile       Plotly dual y-axis for a single plant
iplot_bus_profile         Plotly dual y-axis for all plants at a bus
iplot_vulnerability_table bar chart of the vulnerability lookup table

plot_plant_profile        Matplotlib dual y-axis for a single plant
plot_bus_profile          Matplotlib dual y-axis for all plants at a bus
plot_vulnerability_table  bar chart (Matplotlib)
plot_damage_components    three-panel availability vs temperature (derating / shutdown / combined)

For iplot/plot_plant_profile, pass output of build_plant_damage_df.
For iplot/plot_bus_profile, pass output of build_bus_damage_df.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.lines as mlines
import matplotlib.ticker as mticker

from styles import (
    _SCRIPT_DIR,
    _DAMAGE_COLORS,
    _DAMAGE_SINGLE_COLOR,
    _TEMP_LINE_COLOR,
    _TEMP_BAND_FILL_RGBA,
    _DWT_COLOR,
    _SWT_COLOR,
    _DWT_C,
    _SWT_C,
    _compute_day_ticks,
)

_VULN_CSV = _SCRIPT_DIR.parent / "build_damage_profiles" / "water_temperature_vulnerability.csv"


# ===========================================================================
# Plotly helpers
# ===========================================================================

def _threshold_traces(time_index: pd.DatetimeIndex) -> list:
    """DWT and SWT horizontal reference lines for the secondary (temperature) axis."""
    t = [time_index[0], time_index[-1]]
    return [
        go.Scatter(
            x=t, y=[_DWT_C, _DWT_C],
            mode="lines",
            line=dict(color=_DWT_COLOR, dash="dash", width=1.2),
            name=f"DWT ({_DWT_C:.0f} °C)",
            yaxis="y2",
            hoverinfo="skip",
        ),
        go.Scatter(
            x=t, y=[_SWT_C, _SWT_C],
            mode="lines",
            line=dict(color=_SWT_COLOR, dash="dash", width=1.2),
            name=f"SWT ({_SWT_C:.0f} °C)",
            yaxis="y2",
            hoverinfo="skip",
        ),
    ]


def _apply_layout(
    fig: go.Figure,
    title: str,
    time_index: pd.DatetimeIndex,
    width: int = 1100,
    height: int = 420,
) -> None:
    tickvals, ticktext = _compute_day_ticks(time_index)
    fig.update_layout(
        title=title,
        width=width,
        height=height,
        hovermode="x unified",
        margin=dict(l=60, r=220, t=60, b=60),
        legend=dict(orientation="v", x=1.05, y=1.0, xanchor="left"),
        xaxis=dict(
            title="Time",
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            tickangle=30,
            hoverformat="%m-%d %H:00",
            minor=dict(dtick=86_400_000 // 4, showgrid=False, ticks="inside"),
        ),
        yaxis=dict(
            title="Availability",
            range=[-0.05, 1.05],
            tickformat=".0%",
            side="left",
        ),
        yaxis2=dict(
            title="Lake surface temperature (°C)",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
    )


def _build_damage_figure(
    primary_traces: list,
    secondary_traces: list,
    title: str,
    time_index: pd.DatetimeIndex,
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    fig = go.Figure()
    for trace in primary_traces + secondary_traces + _threshold_traces(time_index):
        fig.add_trace(trace)
    _apply_layout(fig, title, time_index, width, height)
    return fig


# ===========================================================================
# Plotly public API
# ===========================================================================

def iplot_plant_profile(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    """
    Dual y-axis interactive plot for a single nuclear plant.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_plant_damage_df``.
        Index: DatetimeIndex. Columns: ``availability``, ``temp_c``.
    title : str or None
    width, height : int
        Figure dimensions in pixels.

    Returns
    -------
    go.Figure
    """
    time_index = data.index
    plant_label = title or "plant"

    primary = [
        go.Scatter(
            x=time_index, y=data["availability"].values,
            mode="lines", name=plant_label,
            line=dict(color=_DAMAGE_SINGLE_COLOR, width=1.5),
            yaxis="y",
        ),
    ]
    secondary = [
        go.Scatter(
            x=time_index, y=data["temp_c"].values,
            mode="lines", name="Lake temp",
            line=dict(color=_TEMP_LINE_COLOR, width=1.2),
            yaxis="y2",
        ),
    ]
    return _build_damage_figure(
        primary, secondary,
        title=title or "Nuclear availability with Water Temperature",
        time_index=time_index, width=width, height=height,
    )


def iplot_bus_profile(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    """
    Dual y-axis interactive plot for all nuclear plants at a bus.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_bus_damage_df``.
        Aggregate: columns ``availability``, ``temp_min``, ``temp_max``, ``temp_mean``.
        Individual: columns ``{plant_name}``, …, ``temp_min``, ``temp_max``, ``temp_mean``.
    title : str or None
    width, height : int

    Returns
    -------
    go.Figure
    """
    time_index = data.index
    temp_cols = {"temp_min", "temp_max", "temp_mean"}
    is_aggregate = "availability" in data.columns

    primary = []
    if is_aggregate:
        primary.append(go.Scatter(
            x=time_index, y=data["availability"].values,
            mode="lines", name="availability (cap-weighted)",
            line=dict(color=_DAMAGE_SINGLE_COLOR, width=2.0),
            yaxis="y",
        ))
    else:
        plant_cols = [c for c in data.columns if c not in temp_cols]
        for i, col in enumerate(plant_cols):
            primary.append(go.Scatter(
                x=time_index, y=data[col].values,
                mode="lines", name=col,
                line=dict(color=_DAMAGE_COLORS[i % len(_DAMAGE_COLORS)], width=1.5),
                yaxis="y",
            ))

    t_list = list(time_index)
    t_min  = data["temp_min"].values
    t_max  = data["temp_max"].values
    t_mean = data["temp_mean"].values
    secondary = [
        go.Scatter(
            x=t_list, y=t_max,
            mode="lines", line=dict(color="rgba(0,0,0,0)", width=0),
            showlegend=False, yaxis="y2", hoverinfo="skip",
        ),
        go.Scatter(
            x=t_list, y=t_min,
            mode="lines", fill="tonexty", fillcolor=_TEMP_BAND_FILL_RGBA,
            line=dict(color="rgba(0,0,0,0)", width=0),
            name="Temp range", yaxis="y2", hoverinfo="skip",
        ),
        go.Scatter(
            x=t_list, y=t_mean,
            mode="lines", name="Lake temp (mean)",
            line=dict(color=_TEMP_LINE_COLOR, width=1.2),
            yaxis="y2",
        ),
    ]

    return _build_damage_figure(
        primary, secondary,
        title=title or "Nuclear availability with Water Temperature",
        time_index=time_index, width=width, height=height,
    )


def iplot_vulnerability_table() -> go.Figure:
    """
    Bar chart of the water-temperature vulnerability lookup table.

    x-axis : temperature above DWT (°C).  y-axis : fraction inoperable (%).
    A vertical dashed line marks the SWT threshold.
    """
    df = pd.read_csv(_VULN_CSV)
    thresholds = df["threshold"].astype(int).tolist()
    vulns = df["vulnerability"].tolist()
    swt_threshold = _SWT_C - _DWT_C

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=thresholds,
        y=[v * 100 for v in vulns],
        marker_color=_DAMAGE_SINGLE_COLOR,
        name="Vulnerability",
        hovertemplate="%{x} °C above DWT<br>Inoperable: %{y:.1f}%<extra></extra>",
    ))
    fig.add_vline(
        x=swt_threshold - 0.5,
        line_dash="dash",
        line_color=_SWT_COLOR,
        line_width=1.5,
        annotation_text="SWT",
        annotation_position="top right",
        annotation_font_color=_SWT_COLOR,
    )
    fig.update_layout(
        title=dict(text=(
            "Nuclear plant vulnerability factor vs. temperature above DWT"
            f"<br><sup>DWT = {_DWT_C:.0f} °C | SWT = {_SWT_C:.0f} °C</sup>"
        )),
        xaxis=dict(title="Temperature above DWT (°C)", dtick=1, tickmode="linear"),
        yaxis=dict(title="Fraction inoperable (%)", range=[0, 105]),
        showlegend=False,
    )
    return fig


# ===========================================================================
# Matplotlib helpers
# ===========================================================================

def _make_dual_axis_fig(figsize: tuple = (12, 4)):
    fig, ax1 = plt.subplots(figsize=figsize)
    ax2 = ax1.twinx()
    return fig, ax1, ax2


def _apply_mpl_layout(ax1, ax2, title: str | None, time_index: pd.DatetimeIndex,
                      ax1_color: str | None = None) -> None:
    tickvals, ticktext = _compute_day_ticks(time_index)
    ax1.set_xticks(tickvals)
    ax1.set_xticklabels(ticktext, rotation=90, ha="center", fontsize=8)
    ax1.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    ax1.tick_params(axis="x", which="minor", length=3)
    ax1.set_xlabel("Time")
    _ax1_color = ax1_color or _DAMAGE_SINGLE_COLOR
    ax1.set_ylabel("% Operational Capacity", color=_ax1_color)
    ax1.tick_params(axis="y", colors=_ax1_color)
    ax1.set_ylim(-0.05, 1.05)
    ax1.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
    ax2.set_ylabel("Lake surface temperature (°C)", color=_TEMP_LINE_COLOR)
    ax2.tick_params(axis="y", colors=_TEMP_LINE_COLOR)


def _add_threshold_lines(ax2) -> None:
    ax2.axhline(
        _DWT_C, color=_DWT_COLOR, linestyle="--", linewidth=1.0,
        label=f"DWT ({_DWT_C:.0f} °C)", zorder=3,
    )
    ax2.axhline(
        _SWT_C, color=_SWT_COLOR, linestyle="--", linewidth=1.0,
        label=f"SWT ({_SWT_C:.0f} °C)", zorder=3,
    )


def _combine_legends(ax1, ax2) -> None:
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    all_lines, all_labels = lines1 + lines2, labels1 + labels2
    ax1.legend(all_lines, all_labels,
               loc="upper center", bbox_to_anchor=(0.5, -0.35),
               ncol=len(all_lines), fontsize=8, frameon=False)
    ax1.figure.tight_layout()
    ax1.figure.subplots_adjust(bottom=0.22)


# ===========================================================================
# Matplotlib public API
# ===========================================================================

def plot_plant_profile(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (12, 4),
    show_thresholds: bool = True,
) -> tuple:
    """
    Dual y-axis Matplotlib plot for a single nuclear plant.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_plant_damage_df``.
        Index: DatetimeIndex. Columns: ``availability``, ``temp_c``.
    title : str or None
    figsize : tuple

    Returns
    -------
    fig, (ax1, ax2)
    """
    time_index = data.index
    fig, ax1, ax2 = _make_dual_axis_fig(figsize)
    ax1.plot(
        time_index, data["availability"].values,
        color=_DAMAGE_SINGLE_COLOR, linewidth=1.5, label=title or "plant", zorder=4,
    )
    ax2.plot(
        time_index, data["temp_c"].values,
        color=_TEMP_LINE_COLOR, linewidth=1.2, alpha=0.8, label="Lake temp", zorder=2,
    )
    if show_thresholds:
        _add_threshold_lines(ax2)
    _apply_mpl_layout(ax1, ax2, title=title or "Nuclear availability with Water Temperature", time_index=time_index)
    _combine_legends(ax1, ax2)
    return fig, (ax1, ax2)


def plot_bus_profile(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    figsize: tuple = (12, 4),
) -> tuple:
    """
    Dual y-axis Matplotlib plot for all nuclear plants at a bus.

    Parameters
    ----------
    data : pd.DataFrame
        Output of ``build_bus_damage_df``.
        Aggregate: columns ``availability``, ``temp_min``, ``temp_max``, ``temp_mean``.
        Individual: columns ``{plant_name}``, …, ``temp_min``, ``temp_max``, ``temp_mean``.
    title : str or None
    figsize : tuple

    Returns
    -------
    fig, (ax1, ax2)
    """
    time_index = data.index
    temp_cols = {"temp_min", "temp_max", "temp_mean"}
    is_aggregate = "availability" in data.columns

    fig, ax1, ax2 = _make_dual_axis_fig(figsize)

    if is_aggregate:
        ax1.plot(
            time_index, data["availability"].values,
            color=_DAMAGE_SINGLE_COLOR, linewidth=2.0, label=f"Capacity weighted {title or 'bus'}", zorder=4,
        )
    else:
        plant_cols = [c for c in data.columns if c not in temp_cols]
        for i, col in enumerate(plant_cols):
            color = _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
            ax1.plot(time_index, data[col].values, color=color, linewidth=1.5, label=col, alpha=0.9, zorder=4)

    ax2.fill_between(
        time_index, data["temp_min"].values, data["temp_max"].values,
        color=_TEMP_LINE_COLOR, alpha=0.20, label="Temp range", zorder=1,
    )
    ax2.plot(
        time_index, data["temp_mean"].values,
        color=_TEMP_LINE_COLOR, linewidth=1.2, alpha=0.8, label="Lake temp (mean)", zorder=2,
    )
    _add_threshold_lines(ax2)

    ax1_color = _DAMAGE_SINGLE_COLOR
    _apply_mpl_layout(ax1, ax2, title=title or "Nuclear availability with Water Temperature",
                      time_index=time_index, ax1_color=ax1_color)
    _combine_legends(ax1, ax2)
    return fig, (ax1, ax2)


def plot_damage_components(
    figsize: tuple = (15, 5),
    legend_fontsize: int = 12,
    c: float = 1.0,
) -> tuple:
    """
    Three-panel availability vs temperature decomposing the nuclear damage model.

    Panel 1 — Thermal derating: availability = 1 - vulnerability(T - DWT).
    Panel 2 — Regulatory shutdown: availability from water_temperature_regulations.csv
               (steep ramp from 1.0 to 0.0 over the 5°C window below SWT).
    Panel 3 — Combined: minimum availability (maximum damage) across both mechanisms.

    Parameters
    ----------
    c : float
        Vulnerability compression factor. c=1.0 shows only the base curve.
        Any other value overlays the compressed curve on panels 1 and 3.

    Returns
    -------
    fig, axes  (plt.Figure, ndarray of 3 Axes)
    """
    _C_COLOR = "#ff7f0e"

    df = pd.read_csv(_VULN_CSV)
    thresholds = df["threshold"].values.astype(float)
    vulns      = df["vulnerability"].values
    max_thresh = float(thresholds.max())  # 17

    df_reg         = pd.read_csv(_VULN_CSV.parent / "water_temperature_regulations.csv")
    reg_thresholds = df_reg["threshold"].values.astype(float)  # 0→5 (degrees below SWT)
    reg_values     = df_reg["regulation"].values               # 1.0→0.0

    dwt = _DWT_C
    swt = _SWT_C

    T_low  = dwt - 6
    T_high = dwt + max_thresh + 3
    T = np.linspace(T_low, T_high, 1000)

    avail_derate     = np.where(
        T <= dwt, 1.0,
        1.0 - np.interp(T - dwt, thresholds, vulns),
    )
    deg_below_swt    = np.clip(swt - T, 0, 5)
    avail_regulation = 1.0 - np.interp(deg_below_swt, reg_thresholds, reg_values)
    avail_combined   = np.minimum(avail_derate, avail_regulation)

    avail_derate_c   = None
    avail_combined_c = None
    if c != 1.0:
        avail_derate_c = np.where(
            T <= dwt, 1.0,
            1.0 - np.interp(np.clip((T - dwt) * c, 0, 17), thresholds, vulns),
        )
        avail_combined_c = np.minimum(avail_derate_c, avail_regulation)

    panels = [
        (avail_derate,     "(a) Derating from cooling efficiency",      [dwt], [],    True,  avail_derate_c),
        (avail_regulation, "(b) Shutdown from temperature regulations",  [],    [swt], True,  None),
        (avail_combined,   "(c) Combined",                               [dwt], [swt], False, avail_combined_c),
    ]

    n_legend_rows = 1 + (1 if c != 1.0 else 0)
    title_pad = int(legend_fontsize * (n_legend_rows * 2.0 + 0.5) + 10)

    fig, axes = plt.subplots(1, 3, figsize=figsize)

    for ax, (avail, title, dwt_marks, swt_marks, show_legend, avail_c) in zip(axes, panels):
        ax.fill_between(T, avail, color=_DAMAGE_SINGLE_COLOR, alpha=0.35, linewidth=0)
        ax.plot(T, avail, color=_DAMAGE_SINGLE_COLOR, linewidth=1.5)

        if avail_c is not None:
            ax.fill_between(T, avail_c, color=_C_COLOR, alpha=0.20, linewidth=0)
            ax.plot(T, avail_c, color=_C_COLOR, linewidth=1.5, linestyle="--")

        ax.set_xlim(T_low, T_high)
        ax.set_ylim(-0.05, 1.05)
        ax.set_xlabel("Temperature (°C)")
        ax.set_title(title, pad=title_pad)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
        ax.grid(axis="y", linewidth=0.5, alpha=0.4)

        for v in dwt_marks:
            ax.axvline(v, color=_DWT_COLOR, linestyle="--", linewidth=1.0, alpha=0.7)
        for v in swt_marks:
            ax.axvline(v, color=_SWT_COLOR, linestyle="--", linewidth=1.0, alpha=0.7)

        step = 4
        reg_ticks = np.arange(int(np.ceil(T_low / step)) * step, int(T_high) + step, step)
        all_ticks = sorted(set(reg_ticks.tolist()) | {round(v) for v in dwt_marks + swt_marks})

        ax.set_xticks(all_ticks)
        lbl_objs = ax.set_xticklabels([f"{v:.0f}" for v in all_ticks], ha="center")

        for lbl, val in zip(lbl_objs, all_ticks):
            if any(abs(val - v) < 0.5 for v in dwt_marks):
                lbl.set_color(_DWT_COLOR)
            elif any(abs(val - v) < 0.5 for v in swt_marks):
                lbl.set_color(_SWT_COLOR)

        handles = []
        if dwt_marks:
            handles.append(mlines.Line2D([0], [0], color=_DWT_COLOR, linestyle="--",
                                         linewidth=1.0, label="Design Water Temperature"))
        if swt_marks:
            handles.append(mlines.Line2D([0], [0], color=_SWT_COLOR, linestyle="--",
                                         linewidth=1.0, label="Shutdown Water Temperature"))
        if avail_c is not None:
            handles.append(mlines.Line2D([0], [0], color=_C_COLOR, linestyle="--",
                                         linewidth=1.5, label=f"Compressed (c={c})"))
        if handles and show_legend:
            ax.legend(handles=handles, loc="lower left",
                      bbox_to_anchor=(0, 1.02), fontsize=legend_fontsize, framealpha=0.8)

    for ax in axes[1:]:
        ax.tick_params(labelleft=False)

    axes[0].set_ylabel("Availability")
    fig.tight_layout()
    return fig, axes


def plot_vulnerability_table(figsize: tuple = (9, 4)) -> tuple:
    """
    Bar chart of the water-temperature vulnerability lookup table.

    Returns fig, ax.
    """
    df = pd.read_csv(_VULN_CSV)
    thresholds = df["threshold"].astype(int).tolist()
    vulns = df["vulnerability"].tolist()
    swt_threshold = _SWT_C - _DWT_C

    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(thresholds, [v * 100 for v in vulns], color=_DAMAGE_SINGLE_COLOR, edgecolor="white", linewidth=0.5)
    ax.axvline(x=swt_threshold - 0.5, color=_SWT_COLOR, linestyle="--", linewidth=1.5, label="SWT")
    ax.set_xlabel("Temperature above DWT (°C)")
    ax.set_ylabel("Fraction inoperable (%)")
    ax.set_title(
        "Nuclear plant vulnerability factor vs. temperature above DWT\n"
        f"DWT = {_DWT_C:.0f} °C | SWT = {_SWT_C:.0f} °C",
        fontsize=11,
    )
    ax.set_xticks(thresholds)
    ax.set_ylim(0, 105)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    return fig, ax
