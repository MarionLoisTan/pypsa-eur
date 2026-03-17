"""
Shared plotting constants and utilities for build_damage_profiles.

Imported by both:
  plot_nuclear_damage_profiles.py     (Plotly / interactive)
  plot_nuclear_damage_profiles_mpl.py (Matplotlib / reports)

Adding a new damage-profile plot type? Import from here so colours and
tick logic stay consistent across all backends.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from .build_nuclear_damage_profiles import load_damage_config, extract_lake_temp

_SCRIPT_DIR = Path(__file__).parent

_dmg_cfg = load_damage_config()["nuclear"]
_DWT: float = _dmg_cfg["DWT"]   # K — design water temperature
_SWT: float = _dmg_cfg["SWT"]   # K — shutdown water temperature

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

# Display thresholds in °C.
# Derived from damage_config.yaml — updating the YAML automatically updates
# all labels and subtitles across both plotting backends.
_DWT_C: float = _DWT - 273
_SWT_C: float = _SWT - 273


# ---------------------------------------------------------------------------
# Backend-agnostic data helpers
# ---------------------------------------------------------------------------

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
