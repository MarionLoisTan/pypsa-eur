"""
Matplotlib plot functions for nuclear damage profiles (reports / publication).

Mirrors the public API of plot_nuclear_damage_profiles.py but returns
matplotlib Figure / Axes objects for full control over styling and export
(e.g. fig.savefig("output.pdf", dpi=300)).

Public API
----------
plot_plant_profile(plant_name, damage_df, cutout_data, powerplants_df,
                   figsize=(12, 4))  ->  fig, (ax1, ax2)

plot_bus_profile(bus_name, damage_df, cutout_data, powerplants_df,
                 mode='individual', figsize=(12, 4))  ->  fig, (ax1, ax2)

plot_vulnerability_table(figsize=(9, 4))  ->  fig, ax
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

from ._plotting import (
    _DWT,
    _SWT,
    _SCRIPT_DIR,
    _DAMAGE_COLORS,
    _DAMAGE_SINGLE_COLOR,
    _TEMP_LINE_COLOR,
    _DWT_COLOR,
    _SWT_COLOR,
    _DWT_C,
    _SWT_C,
    _get_plant_row,
    _plant_temp_c,
    _compute_day_ticks,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_dual_axis_fig(figsize: tuple = (12, 4)):
    fig, ax1 = plt.subplots(figsize=figsize)
    ax2 = ax1.twinx()
    return fig, ax1, ax2


def _apply_mpl_layout(
    ax1,
    ax2,
    title: str,
    time_index: pd.DatetimeIndex,
) -> None:
    tickvals, ticktext = _compute_day_ticks(time_index)
    ax1.set_xticks(tickvals)
    ax1.set_xticklabels(ticktext, rotation=30, ha="right", fontsize=8)
    ax1.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    ax1.tick_params(axis="x", which="minor", length=3)

    ax1.set_xlabel("Time")
    ax1.set_ylabel("Damage profile (0 = off, 1 = full capacity)")
    ax1.set_ylim(-0.05, 1.05)
    ax1.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))

    ax2.set_ylabel("Lake surface temperature (°C)")
    ax1.set_title(title)
    ax1.figure.tight_layout()


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
    ax1.legend(
        lines1 + lines2, labels1 + labels2,
        loc="lower left", fontsize=8, framealpha=0.8,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def plot_plant_profile(
    plant_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    figsize: tuple = (12, 4),
) -> tuple:
    """
    Dual y-axis Matplotlib plot for a single nuclear plant.

    Parameters
    ----------
    plant_name     : column in damage_df / Name in powerplants_df
    damage_df      : nuclear_damage.csv (index=timestamps, cols=plant names)
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : powerplants_s_{clusters}.csv (nuclear plants only)
    figsize        : passed to plt.subplots

    Returns
    -------
    fig, (ax1, ax2)
    """
    if plant_name not in damage_df.columns:
        raise KeyError(f"'{plant_name}' not found in damage_df columns.")

    plant = _get_plant_row(plant_name, powerplants_df)
    time_index = damage_df.index
    temp_c = _plant_temp_c(plant, cutout_data, time_index)

    fig, ax1, ax2 = _make_dual_axis_fig(figsize)

    ax1.plot(
        time_index, damage_df[plant_name].values,
        color=_DAMAGE_SINGLE_COLOR, linewidth=1.5, label=plant_name, zorder=4,
    )
    ax2.plot(
        time_index, temp_c,
        color=_TEMP_LINE_COLOR, linewidth=1.2, alpha=0.8, label="Lake temp", zorder=2,
    )
    _add_threshold_lines(ax2)
    _apply_mpl_layout(
        ax1, ax2,
        title=f"Nuclear damage profile with Water Temperature — {plant_name}",
        time_index=time_index,
    )
    _combine_legends(ax1, ax2)
    return fig, (ax1, ax2)


def plot_bus_profile(
    bus_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    mode: str = "individual",
    figsize: tuple = (12, 4),
) -> tuple:
    """
    Dual y-axis Matplotlib plot for all nuclear plants at a given bus.

    Parameters
    ----------
    bus_name       : bus identifier matching the 'bus' column in powerplants_df
    damage_df      : nuclear_damage.csv
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : powerplants_s_{clusters}.csv (nuclear plants only)
    mode           : 'individual' — one line per plant
                     'aggregate'  — capacity-weighted mean
    figsize        : passed to plt.subplots

    Returns
    -------
    fig, (ax1, ax2)
    """
    if mode not in ("individual", "aggregate"):
        raise ValueError("mode must be 'individual' or 'aggregate'.")

    bus_plants = powerplants_df[
        (powerplants_df["bus"] == bus_name) &
        (powerplants_df["Name"].isin(damage_df.columns))
    ].copy()
    if bus_plants.empty:
        raise ValueError(f"No nuclear plants found for bus '{bus_name}' in damage_df.")

    time_index = damage_df.index
    capacities = bus_plants["Capacity"].values.astype(float)
    profiles = np.stack(
        [damage_df[row["Name"]].values for _, row in bus_plants.iterrows()], axis=1
    )
    temps_c = np.stack(
        [_plant_temp_c(row, cutout_data, time_index) for _, row in bus_plants.iterrows()],
        axis=1,
    )

    fig, ax1, ax2 = _make_dual_axis_fig(figsize)

    if mode == "individual":
        for i, (_, row) in enumerate(bus_plants.iterrows()):
            color = _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
            ax1.plot(
                time_index, profiles[:, i],
                color=color, linewidth=1.5, label=row["Name"], alpha=0.9, zorder=4,
            )
    else:
        weighted = np.average(profiles, axis=1, weights=capacities)
        ax1.plot(
            time_index, weighted,
            color=_DAMAGE_SINGLE_COLOR, linewidth=2.0,
            label=f"{bus_name} (cap-weighted)", zorder=4,
        )

    t_min  = temps_c.min(axis=1)
    t_max  = temps_c.max(axis=1)
    t_mean = temps_c.mean(axis=1)
    ax2.fill_between(
        time_index, t_min, t_max,
        color=_TEMP_LINE_COLOR, alpha=0.20, label="Temp range", zorder=1,
    )
    ax2.plot(
        time_index, t_mean,
        color=_TEMP_LINE_COLOR, linewidth=1.2, alpha=0.8, label="Lake temp (mean)", zorder=2,
    )
    _add_threshold_lines(ax2)

    mode_label = "individual plants" if mode == "individual" else "capacity-weighted aggregate"
    _apply_mpl_layout(
        ax1, ax2,
        title=f"Nuclear damage profiles with Water Temperature — bus {bus_name} ({mode_label})",
        time_index=time_index,
    )
    _combine_legends(ax1, ax2)
    return fig, (ax1, ax2)


def plot_vulnerability_table(figsize: tuple = (9, 4)) -> tuple:
    """
    Bar chart of the water-temperature vulnerability lookup table.

    x-axis   : temperature above DWT (°C), 0–17
    y-axis   : fraction inoperable (%)
    Subtitle : DWT and SWT values in °C (auto-updates if constants change)
    A vertical dashed line marks the SWT threshold.

    Returns
    -------
    fig, ax
    """
    df = pd.read_csv(_SCRIPT_DIR / "water_temperature_vulnerability.csv")
    thresholds = df["threshold"].astype(int).tolist()
    vulns = df["vulnerability"].tolist()
    swt_threshold = _SWT - _DWT  # °C above DWT

    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(
        thresholds, [v * 100 for v in vulns],
        color=_DAMAGE_SINGLE_COLOR, edgecolor="white", linewidth=0.5,
    )
    ax.axvline(
        x=swt_threshold - 0.5,
        color=_SWT_COLOR, linestyle="--", linewidth=1.5, label="SWT",
    )
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
