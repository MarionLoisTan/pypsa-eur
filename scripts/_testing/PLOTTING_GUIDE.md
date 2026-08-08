# Plotting guide — `scripts/_testing`

A reference for adding new plots and maintaining existing ones consistently.

---

## Architecture: three layers

| Layer | Rule |
|-------|------|
| **Builders** | Pure data — no matplotlib/plotly imports. Return a DataFrame or dict of DataFrames. |
| **Styles** | Colour constants and visual utilities only. |
| **Plot functions** | Rendering only. Always receive a DataFrame as first argument. |

The separation means plot functions never contain aggregation logic, and builders never contain rendering logic.

---

## File map

| File | Layer | Owns |
|------|-------|------|
| `styles.py` | Style | colour constants, `_compute_day_ticks`, `_darken_hex`, `_hex_to_rgba`, `show_fullscreen` |
| `builders.py` | Builder | all `build_*` functions, `fill_short_gaps`, `build_cf_temp_aligned_df`, `compute_stats_diff`, `format_stats_df` |
| `plot_cf.py` | Plot | CF time-series and scatter, both mpl and iplot |
| `plot_damage.py` | Plot | nuclear damage profiles and component decomposition, both backends |
| `plot_flows.py` | Plot | net AC flow plots |
| `plot_network.py` | Plot | p_max_pu, CF vs p_max_pu, aggregated CF vs p_max_pu — both backends |
| `plot_stats.py` | Plot | statistics bar charts, network map, energy balance map (mpl and pydeck) |
| `plot_combo.py` | Self-contained | combination time-series profile plots for PyPSA scenario comparison; intentionally self-contained — includes its own builders (`format_ts_df`, `format_ts_diff_df`, `build_eb_diff_by_bus`, `build_storage_dict`) and renderers (`plot_combination_profile`) in one module; not part of the three-layer architecture by design |
| `damage_computation_functions.py` | — | legacy standalone script; not part of the three-layer architecture |

---

## Data Input

Two standard input shapes used across most functions.

**Scenario dict** — CF / damage workflow:
```python
{
    "year": int,
    "damage_df": pd.DataFrame,       # cols=plants, index=timestamps, values∈[0,1]
    "powerplants_df": pd.DataFrame,  # cols: Name, lat, lon, Capacity, bus
    "cf_actual": pd.DataFrame,       # cols: unit_name, capacity_factor, installed_capacity_mw, ...
    "mapping": pd.DataFrame,         # cols: Name, unit_name
    "cutout_data": xr.Dataset,       # ERA5 variables incl. lake_s_temp
}
```

Multi-variant scenarios are created by shallow copy — builders only see `cf_actual`:
```python
filled_scenarios   = [{**s, "cf_actual": s["cf_actual_filled"]} for s in scenarios]
filtered_scenarios = [{**s, "cf_actual": s["cf_filtered"]}      for s in scenarios]
```

**Networks dict** — PyPSA result workflow:
```python
{"scenario_label": pypsa.Network, ...}
```

---

## Builder conventions

Write a builder when the plot needs:
- multiple raw inputs joined or aggregated together
- capacity-weighted aggregation across units or buses
- multi-year / multi-scenario iteration

A plot that receives one clean DataFrame does not need a builder.

**Scope parameter** — handle the common flexibility axes in the builder, not the plot function:
```python
build_p_max_pu_df(networks, carriers, scope="per_bus")    # one column per bus
build_p_max_pu_df(networks, carriers, scope="aggregate")  # capacity-weighted single column
build_p_max_pu_df(networks, carriers, scope="overlay")    # timestamps remapped to year 2000
```

---

## Plot function signatures

Every plot function follows this structure:

```python
def plot_topic(
    data: pd.DataFrame,              # always first — output of a builder
    *,                               # everything below is keyword-only
    title: str | None = None,
    date_range: tuple | None = None, # ("YYYY-MM-DD", "YYYY-MM-DD") or ("MM-DD", "MM-DD")
    figsize: tuple = (10, 4),
    ax: plt.Axes | None = None,      # pass to draw into an existing axes
) -> tuple[plt.Figure, plt.Axes]:

def iplot_topic(
    data: pd.DataFrame,
    *,
    title: str | None = None,
    fig: go.Figure | None = None,    # pass to add traces to an existing figure
) -> go.Figure:
```

Rules:
- `*` enforces keyword-only after `data` — call sites are self-documenting
- `ax=None` / `fig=None` enables composition (e.g. slotting into a subplot grid)
- mpl functions always return `(fig, ax)` even when `ax` was passed in
- mpl and Plotly versions of the same plot live in the **same file**

---

## Naming conventions

| Pattern | Convention | Example |
|---------|-----------|---------|
| Builder | `build_<noun>_df` | `build_p_max_pu_df` |
| mpl plot | `plot_<topic>` | `plot_cf_comparison` |
| Plotly plot | `iplot_<topic>` | `iplot_cf_comparison` |
| Internal helper | `_<name>` | `_get_trace_style` |
| Style constant | `_ALL_CAPS` | `_DAMAGE_COLORS` |

---

## Colour and style rules

- All colour constants live in `styles.py` — never define colours inside plot functions
- Cycle through `_DAMAGE_COLORS` for multi-scenario / multi-plant traces
- Temperature traces: `_TEMP_LINE_COLOR`, `_TEMP_BAND_FILL_RGBA`
- Threshold lines: `_DWT_COLOR` (`#CC6600`, dark orange), `_SWT_COLOR` (`crimson`)
- Year-keyed multi-scenario CF traces: `plotly.colors.qualitative.Plotly`, indexed by year order — defined locally in `plot_cf.py` as `_PLOTLY_COLORS`

---

## Quick reference

| Plot function(s) | Builder | Raw inputs to builder |
|-----------------|---------|----------------------|
| `plot_plant_cf_comparison` / `_mpl` | *(none — takes raw inputs directly)* | `damage_df`, `cf_actual`, `mapping`; or `scenarios` list |
| `cf_to_temp` | `build_cf_sources`, `build_multi_scenario_sources` | `scenarios` list, `cutout_data` dict, `powerplants_df` |
| `plot_plant_profile` / `iplot_plant_profile` | *(none — takes output of `build_plant_damage_df`)* | `damage_df`, `cutout_data`, `powerplants_df` |
| `plot_bus_profile` / `iplot_bus_profile` | *(none — takes output of `build_bus_damage_df`)* | `damage_df`, `cutout_data`, `powerplants_df` |
| `plot_vulnerability_table` / `iplot_vulnerability_table` | *(none — loads CSV internally)* | — |
| `plot_damage_components` | *(none — loads CSVs internally)* | — |
| `plot_bus_net_flow` / `iplot_bus_net_flow` | `build_bus_net_flow_df` | `networks` dict |
| `plot_p_max_pu` / `iplot_p_max_pu` | `build_p_max_pu_df` | `networks` dict, `carriers` list |
| `plot_cf_pmax` / `iplot_cf_pmax` | `build_cf_pmax_df` | `n.statistics()` output (`ts`), `networks` dict |
| `plot_agg_cf_pmax` / `iplot_agg_cf_pmax` | `build_agg_cf_pmax_df` | `networks` dict, `carriers` list |
| `plot_stats_bar` / `iplot_stats_bar` | `compute_stats_diff` → `format_stats_df` | `n.statistics()` output |
| `eb_imap_network` | *(none — calls `n.statistics.energy_balance()` internally)* | `pypsa.Network` |
| `eb_plot_network` | *(none — calls `n.statistics.energy_balance()` internally)* | `pypsa.Network` |
| `plot_network_map` | *(none — takes `pypsa.Network` directly)* | `pypsa.Network` |
| `plot_combination_profile` | `format_ts_df`, `format_ts_diff_df`, `build_eb_diff_by_bus`, `build_storage_dict` | `networks` dict, `pypsa.Network` components |