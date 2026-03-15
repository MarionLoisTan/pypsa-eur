"""
Interactive Plotly plot functions for nuclear damage profiles.

Public API
----------
plot_plant_profile(plant_name, damage_df, cutout_data, powerplants_df)
    Dual y-axis interactive plot for a single nuclear plant.
    Left axis : damage profile [0, 1]
    Right axis: lake surface temperature (°C) with DWT / SWT threshold lines

plot_bus_profile(bus_name, damage_df, cutout_data, powerplants_df, mode='individual')
    Same layout for all nuclear plants at a given bus.
    mode='individual' : one line per plant (uniform linewidth)
    mode='aggregate'  : capacity-weighted mean damage line
    Right axis        : min/max temperature band across plants

plot_vulnerability_table()
    Bar chart of the water-temperature vulnerability lookup table
    (fraction inoperable vs. °C above DWT).

All functions return a plotly.graph_objects.Figure.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .build_nuclear_damage_profiles import (
    DWT,
    SWT,
    _SCRIPT_DIR,
    extract_lake_temp,
)

# ---------------------------------------------------------------------------
# Colour constants
# ---------------------------------------------------------------------------
# Damage traces  — cool qualitative palette (blues / greens / purples)
_DAMAGE_COLORS = [
    "#1f77b4",  # muted blue
    "#2ca02c",  # cooked asparagus green
    "#9467bd",  # muted purple
    "#17becf",  # blue-teal
    "#8c564b",  # chestnut brown
    "#e377c2",  # raspberry yogurt pink
    "#bcbd22",  # curry yellow-green
    "#7f7f7f",  # middle gray
]
_DAMAGE_SINGLE_COLOR = "#1f77b4"  # single-plant / aggregate line

# Temperature traces — warm palette
_TEMP_LINE_COLOR = "darkorange"
_TEMP_BAND_FILL = "rgba(255, 160, 50, 0.20)"
_TEMP_BAND_LINE = "rgba(0,0,0,0)"  # invisible band boundary line

# Threshold lines on the temperature axis
_DWT_COLOR = "mediumseagreen"
_SWT_COLOR = "crimson"

# Display thresholds in °C
_DWT_C = DWT - 273
_SWT_C = SWT - 273

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_plant_row(plant_name: str, powerplants_df: pd.DataFrame) -> pd.Series:
    mask = powerplants_df["Name"] == plant_name
    if not mask.any():
        raise KeyError(f"Plant '{plant_name}' not found in powerplants_df.")
    return powerplants_df.loc[mask].iloc[0]


def _plant_temp_c(plant_row: pd.Series, cutout_data, time_index: pd.DatetimeIndex) -> np.ndarray:
    raw = extract_lake_temp(cutout_data, plant_row["lat"], plant_row["lon"], time_index)
    return raw - 273.0


def _threshold_traces(time_index: pd.DatetimeIndex) -> list:
    """Return DWT and SWT horizontal traces for the secondary (temperature) y-axis."""
    t = list(time_index)
    return [
        go.Scatter(
            x=[t[0], t[-1]], y=[_DWT_C, _DWT_C],
            mode="lines",
            line=dict(color=_DWT_COLOR, dash="dash", width=1.2),
            name=f"DWT ({_DWT_C:.0f} °C)",
            yaxis="y2",
            hoverinfo="skip",
        ),
        go.Scatter(
            x=[t[0], t[-1]], y=[_SWT_C, _SWT_C],
            mode="lines",
            line=dict(color=_SWT_COLOR, dash="dash", width=1.2),
            name=f"SWT ({_SWT_C:.0f} °C)",
            yaxis="y2",
            hoverinfo="skip",
        ),
    ]


def _apply_layout(fig: go.Figure, title: str, time_index: pd.DatetimeIndex) -> None:
    """Apply shared layout: title, axis labels, day ticks, hover mode."""
    fig.update_layout(
        title=title,
        hovermode="x unified",
        legend=dict(orientation="v", x=1.08, y=1.0),
        xaxis=dict(
            title="Time",
            tickformat="%b %d",
            dtick=86_400_000,          # major tick every day (ms)
            minor=dict(
                dtick=86_400_000 // 4, # minor tick every 6 h
                showgrid=False,
                ticks="inside",
            ),
            tickangle=-30,
        ),
        yaxis=dict(
            title="Damage profile",
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def plot_plant_profile(
    plant_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
) -> go.Figure:
    """
    Dual y-axis interactive plot for a single nuclear plant.

    Parameters
    ----------
    plant_name     : name matching a column in damage_df and a row in powerplants_df
    damage_df      : DataFrame from nuclear_damage.csv (index=timestamps, cols=plant names)
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : DataFrame from powerplants_s_{clusters}.csv (nuclear plants)

    Returns
    -------
    plotly.graph_objects.Figure
    """
    if plant_name not in damage_df.columns:
        raise KeyError(f"'{plant_name}' not found in damage_df columns.")

    plant = _get_plant_row(plant_name, powerplants_df)
    time_index = damage_df.index
    temp_c = _plant_temp_c(plant, cutout_data, time_index)

    fig = go.Figure()

    # Damage profile (primary y)
    fig.add_trace(go.Scatter(
        x=time_index, y=damage_df[plant_name].values,
        mode="lines",
        name=plant_name,
        line=dict(color=_DAMAGE_SINGLE_COLOR, width=1.5),
        yaxis="y",
    ))

    # Lake temperature (secondary y)
    fig.add_trace(go.Scatter(
        x=time_index, y=temp_c,
        mode="lines",
        name="Lake temp",
        line=dict(color=_TEMP_LINE_COLOR, width=1.2),
        yaxis="y2",
    ))

    # DWT / SWT threshold lines
    for trace in _threshold_traces(time_index):
        fig.add_trace(trace)

    _apply_layout(fig, title=f"Nuclear damage profile — {plant_name}", time_index=time_index)
    return fig


def plot_bus_profile(
    bus_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    mode: str = "individual",
) -> go.Figure:
    """
    Dual y-axis interactive plot for all nuclear plants assigned to a given bus.

    Parameters
    ----------
    bus_name       : bus identifier (e.g. 'FR0 1') matching the 'bus' column
    damage_df      : DataFrame from nuclear_damage.csv
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : DataFrame from powerplants_s_{clusters}.csv (nuclear plants)
    mode           : 'individual' — one line per plant (uniform linewidth, legend-togglable)
                     'aggregate'  — single capacity-weighted mean damage line

    Returns
    -------
    plotly.graph_objects.Figure
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

    profiles = np.stack([damage_df[row["Name"]].values for _, row in bus_plants.iterrows()], axis=1)
    temps_c = np.stack([_plant_temp_c(row, cutout_data, time_index) for _, row in bus_plants.iterrows()], axis=1)

    fig = go.Figure()

    # --- Damage traces (primary y) ---
    if mode == "individual":
        for i, (_, row) in enumerate(bus_plants.iterrows()):
            color = _DAMAGE_COLORS[i % len(_DAMAGE_COLORS)]
            fig.add_trace(go.Scatter(
                x=time_index, y=profiles[:, i],
                mode="lines",
                name=row["Name"],
                line=dict(color=color, width=1.5),
                yaxis="y",
            ))
    else:  # aggregate
        weighted = np.average(profiles, axis=1, weights=capacities)
        fig.add_trace(go.Scatter(
            x=time_index, y=weighted,
            mode="lines",
            name=f"{bus_name} (cap-weighted)",
            line=dict(color=_DAMAGE_SINGLE_COLOR, width=2.0),
            yaxis="y",
        ))

    # --- Temperature band (secondary y) ---
    t_min = temps_c.min(axis=1)
    t_max = temps_c.max(axis=1)
    t_mean = temps_c.mean(axis=1)
    t_list = list(time_index)

    # Upper boundary of band (invisible line, used as fill reference)
    fig.add_trace(go.Scatter(
        x=t_list, y=t_max,
        mode="lines",
        line=dict(color=_TEMP_BAND_LINE, width=0),
        showlegend=False,
        yaxis="y2",
        hoverinfo="skip",
    ))
    # Lower boundary — fills back to upper
    fig.add_trace(go.Scatter(
        x=t_list, y=t_min,
        mode="lines",
        fill="tonexty",
        fillcolor=_TEMP_BAND_FILL,
        line=dict(color=_TEMP_BAND_LINE, width=0),
        name="Temp range",
        yaxis="y2",
        hoverinfo="skip",
    ))
    # Mean temperature line
    fig.add_trace(go.Scatter(
        x=t_list, y=t_mean,
        mode="lines",
        name="Lake temp (mean)",
        line=dict(color=_TEMP_LINE_COLOR, width=1.2),
        yaxis="y2",
    ))

    # DWT / SWT threshold lines
    for trace in _threshold_traces(time_index):
        fig.add_trace(trace)

    mode_label = "individual plants" if mode == "individual" else "capacity-weighted aggregate"
    _apply_layout(
        fig,
        title=f"Nuclear damage profiles — bus {bus_name} ({mode_label})",
        time_index=time_index,
    )
    return fig


def plot_vulnerability_table() -> go.Figure:
    """
    Bar chart of the water-temperature vulnerability lookup table.

    x-axis : temperature above DWT (°C), range 0–17
    y-axis : fraction inoperable (displayed as %)
    A vertical dashed line marks the SWT threshold (= SWT - DWT °C above DWT),
    which is where full shutdown begins.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    csv_path = _SCRIPT_DIR / "water_temperature_vulnerability.csv"
    df = pd.read_csv(csv_path)
    thresholds = df["threshold"].astype(int).tolist()
    vulns = df["vulnerability"].tolist()

    swt_threshold = SWT - DWT  # °C above DWT where full shutdown begins

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=thresholds,
        y=[v * 100 for v in vulns],  # display as %
        marker_color="#1f77b4",
        name="Vulnerability",
        hovertemplate="%{x} °C above DWT<br>Inoperable: %{y:.1f}%<extra></extra>",
    ))

    # Vertical line at SWT threshold
    fig.add_vline(
        x=swt_threshold - 0.5,  # offset to align with bar boundary
        line_dash="dash",
        line_color=_SWT_COLOR,
        line_width=1.5,
        annotation_text="SWT",
        annotation_position="top right",
        annotation_font_color=_SWT_COLOR,
    )

    fig.update_layout(
        title="Nuclear plant vulnerability vs. temperature above DWT",
        xaxis=dict(
            title="Temperature above DWT (°C)",
            dtick=1,
            tickmode="linear",
        ),
        yaxis=dict(
            title="Fraction inoperable (%)",
            range=[0, 105],
        ),
        showlegend=False,
    )

    return fig
