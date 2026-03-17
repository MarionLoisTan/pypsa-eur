"""
Interactive Plotly plot functions for nuclear damage profiles.

Public API
----------
plot_plant_profile(plant_name, damage_df, cutout_data, powerplants_df,
                   width=1100, height=420)
    Dual y-axis interactive plot for a single nuclear plant.
    Left axis : damage profile [0, 1]
    Right axis: lake surface temperature (°C) with DWT / SWT threshold lines

plot_bus_profile(bus_name, damage_df, cutout_data, powerplants_df,
                 mode='individual', width=1100, height=420)
    Same layout for all nuclear plants at a given bus.
    mode='individual' : one legend-togglable line per plant
    mode='aggregate'  : single capacity-weighted mean line
    Right axis        : min/max temperature band across all bus plants

plot_vulnerability_table()
    Bar chart of the water-temperature vulnerability lookup table
    (fraction inoperable vs. °C above DWT), with DWT / SWT subtitle.

All functions return a plotly.graph_objects.Figure.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from ._plotting import (
    _DWT,
    _SWT,
    _SCRIPT_DIR,
    _DAMAGE_COLORS,
    _DAMAGE_SINGLE_COLOR,
    _TEMP_LINE_COLOR,
    _TEMP_BAND_FILL_RGBA,
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
            minor=dict(
                dtick=86_400_000 // 4,  # minor tick every 6 h (ms)
                showgrid=False,
                ticks="inside",
            ),
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


def _build_damage_figure(
    primary_traces: list,
    secondary_traces: list,
    title: str,
    time_index: pd.DatetimeIndex,
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    """
    Assemble a dual y-axis figure from pre-built trace lists.

    primary_traces   → left y-axis  (damage profiles, yaxis="y")
    secondary_traces → right y-axis (temperature,     yaxis="y2")

    DWT / SWT threshold lines are appended automatically.
    """
    fig = go.Figure()
    for trace in primary_traces + secondary_traces + _threshold_traces(time_index):
        fig.add_trace(trace)
    _apply_layout(fig, title, time_index, width, height)
    return fig


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def plot_plant_profile(
    plant_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    """
    Dual y-axis interactive plot for a single nuclear plant.

    Parameters
    ----------
    plant_name     : column in damage_df / Name in powerplants_df
    damage_df      : nuclear_damage.csv (index=timestamps, cols=plant names)
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : powerplants_s_{clusters}.csv (nuclear plants only)
    width, height  : figure dimensions in pixels

    Returns
    -------
    plotly.graph_objects.Figure
    """
    if plant_name not in damage_df.columns:
        raise KeyError(f"'{plant_name}' not found in damage_df columns.")

    plant = _get_plant_row(plant_name, powerplants_df)
    time_index = damage_df.index
    temp_c = _plant_temp_c(plant, cutout_data, time_index)

    primary = [
        go.Scatter(
            x=time_index, y=damage_df[plant_name].values,
            mode="lines", name=plant_name,
            line=dict(color=_DAMAGE_SINGLE_COLOR, width=1.5),
            yaxis="y",
        ),
    ]
    secondary = [
        go.Scatter(
            x=time_index, y=temp_c,
            mode="lines", name="Lake temp",
            line=dict(color=_TEMP_LINE_COLOR, width=1.2),
            yaxis="y2",
        ),
    ]
    return _build_damage_figure(
        primary, secondary,
        title=f"Nuclear damage profile with Water Temperature — {plant_name}",
        time_index=time_index, width=width, height=height,
    )


def plot_bus_profile(
    bus_name: str,
    damage_df: pd.DataFrame,
    cutout_data,
    powerplants_df: pd.DataFrame,
    mode: str = "individual",
    width: int = 1100,
    height: int = 420,
) -> go.Figure:
    """
    Dual y-axis interactive plot for all nuclear plants at a given bus.

    Parameters
    ----------
    bus_name       : bus identifier matching the 'bus' column in powerplants_df
    damage_df      : nuclear_damage.csv
    cutout_data    : xr.Dataset  (cutout.data)
    powerplants_df : powerplants_s_{clusters}.csv (nuclear plants only)
    mode           : 'individual' — one legend-togglable line per plant
                     'aggregate'  — single capacity-weighted mean line
    width, height  : figure dimensions in pixels

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
    profiles = np.stack(
        [damage_df[row["Name"]].values for _, row in bus_plants.iterrows()], axis=1
    )
    temps_c = np.stack(
        [_plant_temp_c(row, cutout_data, time_index) for _, row in bus_plants.iterrows()],
        axis=1,
    )

    # Damage traces (primary y)
    primary = []
    if mode == "individual":
        for i, (_, row) in enumerate(bus_plants.iterrows()):
            primary.append(go.Scatter(
                x=time_index, y=profiles[:, i],
                mode="lines", name=row["Name"],
                line=dict(color=_DAMAGE_COLORS[i % len(_DAMAGE_COLORS)], width=1.5),
                yaxis="y",
            ))
    else:
        weighted = np.average(profiles, axis=1, weights=capacities)
        primary.append(go.Scatter(
            x=time_index, y=weighted,
            mode="lines", name=f"{bus_name} (cap-weighted)",
            line=dict(color=_DAMAGE_SINGLE_COLOR, width=2.0),
            yaxis="y",
        ))

    # Temperature band (secondary y)
    t_list = list(time_index)
    t_min  = temps_c.min(axis=1)
    t_max  = temps_c.max(axis=1)
    t_mean = temps_c.mean(axis=1)
    secondary = [
        go.Scatter(          # invisible upper boundary — fill reference
            x=t_list, y=t_max,
            mode="lines", line=dict(color="rgba(0,0,0,0)", width=0),
            showlegend=False, yaxis="y2", hoverinfo="skip",
        ),
        go.Scatter(          # lower boundary fills back to upper
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

    mode_label = "individual plants" if mode == "individual" else "capacity-weighted aggregate"
    return _build_damage_figure(
        primary, secondary,
        title=f"Nuclear damage profiles with Water Temperature — bus {bus_name} ({mode_label})",
        time_index=time_index, width=width, height=height,
    )


def plot_vulnerability_table() -> go.Figure:
    """
    Bar chart of the water-temperature vulnerability lookup table.

    x-axis   : temperature above DWT (°C), 0–17
    y-axis   : fraction inoperable (%)
    Subtitle : DWT and SWT values in °C (auto-updates if constants change)
    A vertical dashed line marks the SWT threshold.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    df = pd.read_csv(_SCRIPT_DIR / "water_temperature_vulnerability.csv")
    thresholds = df["threshold"].astype(int).tolist()
    vulns = df["vulnerability"].tolist()
    swt_threshold = _SWT - _DWT  # °C above DWT

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=thresholds,
        y=[v * 100 for v in vulns],
        marker_color=_DAMAGE_SINGLE_COLOR,
        name="Vulnerability",
        hovertemplate="%{x} °C above DWT<br>Inoperable: %{y:.1f}%<extra></extra>",
    ))
    fig.add_vline(
        x=swt_threshold - 0.5,  # sits on the boundary before the SWT bar
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
