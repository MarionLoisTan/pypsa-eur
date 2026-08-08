"""
Shared colour constants and visual utilities for _testing plotting scripts.

No data logic or matplotlib/plotly figure construction here — only constants
and pure visual helpers (colour transforms, tick computation, etc.).
"""

import colorsys
from pathlib import Path

import matplotlib as mpl
import pandas as pd
import plotly.io as pio

mpl.rcParams.update({
    # "font.family":     "Arial",
    "axes.titlesize":  14,
    "axes.labelsize":  14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 12,
})

from scripts.build_damage_profiles.build_nuclear_damage_profiles import load_damage_config

_SCRIPT_DIR = Path(__file__).parent

_nuclear_cfg: dict = {}


def _ensure_nuclear_cfg() -> None:
    if not _nuclear_cfg:
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
_DWT_COLOR = "#CC6600"
_SWT_COLOR = "crimson"

# _DWT, _SWT, _DWT_C and _SWT_C are provided via __getattr__ (lazy-loaded on first access).


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
    pio.show(fig, renderer="browser")
