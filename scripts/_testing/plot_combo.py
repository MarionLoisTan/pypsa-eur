import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
from matplotlib.ticker import FuncFormatter, FixedLocator
from functools import reduce


def build_storage_dict(nc, network):
    battery = nc.storage_units_t.state_of_charge[network]
    battery.columns = battery.columns.str.rsplit(" ", n=1).str[0]  # "GB0 0"
    battery = battery.T.groupby(level=0).sum().T

    h2 = nc.stores_t.e.filter(like="H2")[network]
    h2.columns = h2.columns.str.rsplit(" ", n=1).str[0]
    h2 = h2.T.groupby(level=0).sum().T

    return {"battery": battery, "H2": h2}


def _apply_carrier_groups(data_dict: dict, carrier_groups: dict) -> dict:
    result = {}
    for label, spec in carrier_groups.items():
        carriers = [spec] if isinstance(spec, str) else spec
        parts = [data_dict[c] for c in carriers if c in data_dict]
        if parts:
            result[label] = reduce(lambda a, b: a.add(b, fill_value=0), parts)
    return result

def format_ts_df(nc, stat="energy_balance", **stat_kwargs):
    result = getattr(nc.statistics, stat)(
        groupby=["bus", "carrier", "bus_carrier"],
        groupby_time=False,
        nice_names=False,
        **stat_kwargs,
    )
    df = result.reset_index()
    meta_cols = [c for c in df.columns if not isinstance(c, pd.Timestamp)]
    ts_cols   = [c for c in df.columns if isinstance(c, pd.Timestamp)]
    df = df.melt(id_vars=meta_cols, value_vars=ts_cols, var_name="snapshot", value_name="value")
    return df

def format_ts_diff_df(networks, stat="energy_balance", base="base", **stat_kwargs):
    if base not in networks:
        raise ValueError(f"Base label {base!r} not found in networks.")
    all_dfs = []
    for label, n in networks.items():
        df = format_ts_df(n, stat=stat, **stat_kwargs)
        df["network"] = label
        all_dfs.append(df)
    combined = pd.concat(all_dfs, ignore_index=True)
    merge_keys = ["bus", "carrier", "bus_carrier", "snapshot"]
    base_vals = (
        combined[combined["network"] == base][merge_keys + ["value"]]
        .rename(columns={"value": "_base_value"})
    )
    combined = combined.merge(base_vals, on=merge_keys, how="left")
    combined["_base_value"] = combined["_base_value"].fillna(0)
    combined["value"]       = combined["value"].fillna(0)
    combined["value_diff"]  = combined["value"] - combined["_base_value"]
    combined.drop(columns="_base_value", inplace=True)
    return combined

def build_eb_diff_by_bus(
    ts_diff: pd.DataFrame,
    carrier: str,
    network: str,
) -> pd.DataFrame:
    """Pivot energy balance diff for one carrier/network to (time × bus)."""
    mask = (
        (ts_diff["carrier"] == carrier) &
        (ts_diff["network"] == network) &
        (ts_diff["bus_carrier"] == "AC")
    )
    raw = ts_diff[mask]
    return raw.pivot_table(index="snapshot", columns="bus", values="value_diff", aggfunc="sum")


def _slice_date(d: dict, date_range) -> dict:
    if date_range is None:
        return d
    s, e = date_range
    return {k: df.loc[s:e] for k, df in d.items()}


def _agg_buses(d: dict) -> dict:
    return {k: df.sum(axis=1).to_frame("fleet") for k, df in d.items()}

def _render_heatmap_row(fig, ax, label, df, norm, cmap, unit="", fontsize=9, label_gap=5):
    if df.empty:
        ax.axis("off")
    else:
        ax.imshow(df.T.values, aspect="auto", cmap=cmap,
                  interpolation="nearest", norm=norm)
        ax.set_yticks(range(len(df.columns)))
        if len(df.columns) > 1:
            ax.set_yticklabels(df.columns, fontsize=fontsize - 1)
        else:
            ax.set_yticklabels([])
        pos = ax.get_position()
        cax = fig.add_axes([0.895, pos.y0 + pos.height * 0.1, 0.015, pos.height * 0.8])
        fig.colorbar(plt.cm.ScalarMappable(cmap=cmap, norm=norm), cax=cax, label=unit)
    ax.set_ylabel("")
    ax.annotate(label, xy=(0, 0.5), xycoords="axes fraction",
                xytext=(-label_gap, 0), textcoords="offset points",
                ha="right", va="center", fontsize=fontsize, annotation_clip=False)



def _render_area_row(ax, label, df, ylim, *,
                     bus_name=None, unit="",
                     color="steelblue", neg_color="salmon", fontsize=9, label_gap=5):
    x = np.arange(len(df))
    y = df.iloc[:, 0].values
    ax.fill_between(x, y, 0, where=(y >= 0), alpha=0.7, color=color)
    ax.fill_between(x, y, 0, where=(y < 0), alpha=0.7, color=neg_color)
    ax.axhline(0, color="black", linewidth=0.5, linestyle="--")
    ax.set_ylim(ylim)

    ax.set_ylabel("")
    ax.annotate(label, xy=(0, 0.5), xycoords="axes fraction",
                xytext=(-label_gap, 0), textcoords="offset points",
                ha="right", va="center", fontsize=fontsize, annotation_clip=False)

    if bus_name is not None:
        ax.annotate(bus_name, xy=(0, 0.5), xycoords="axes fraction",
                    xytext=(-4, 0), textcoords="offset points",
                    ha="right", va="center", fontsize=fontsize - 1, clip_on=False)

    # single tick at max (and min for symmetric), minor at midpoints
    is_sym = ylim[0] < 0
    major_ticks = [ylim[1]] if is_sym else [ylim[1]]
    minor_ticks = [ylim[0] / 2, ylim[1] / 2] if is_sym else [ylim[1] / 2]

    ax.yaxis.tick_right()
    ax.set_yticks(major_ticks)
    ax.yaxis.set_major_formatter(
        FuncFormatter(lambda v, _: f"{v:.3g} {unit}" if unit else f"{v:.3g}")
    )
    ax.yaxis.set_minor_locator(FixedLocator(minor_ticks))
    ax.tick_params(axis="y", labelsize=fontsize - 2)

    ax.grid(True, axis="y", which="major", alpha=0.3, linestyle="--", linewidth=0.5)
    ax.grid(True, axis="y", which="minor", alpha=0.2, linestyle=":",  linewidth=0.4)

def plot_combination_profile(
    carrier_groups: dict,
    components: dict,
    *,
    date_range=None,
    aggregate_buses=False,
    row_height=1.5,
    fig_width=16,
    title=None,
    show_day_ticks=False,
    fontsize=9,
    label_gap=5,
    component_gap=0.35,
):
    all_carriers = [c for v in carrier_groups.values()
                    for c in ([v] if isinstance(v, str) else v)]

    # ── Step 1: build render specs ────────────────────────────────────────────
    render_specs = []
    for comp_key, comp in components.items():
        ctype  = comp["type"]
        render = comp.get("render", "heatmap")
        cmap   = comp.get("cmap", "YlOrRd")
        sym    = comp.get("symmetric", False)
        rh     = comp.get("row_height", row_height)
        vg             = comp.get("vmax_group")
        unit           = comp.get("unit", "")
        color          = comp.get("color", "steelblue")
        neg_color      = comp.get("neg_color", "salmon")
        vmax_override  = comp.get("vmax")
        multiplier     = comp.get("multiplier", 1.0)
        data           = comp["data"]

        if ctype == "carrier_dict":
            raw     = _slice_date({k: v for k, v in data.items() if k in all_carriers}, date_range)
            grouped = _apply_carrier_groups(raw, carrier_groups)
        elif ctype == "ts_diff":
            raw     = {c: build_eb_diff_by_bus(data, c, comp["network"]) for c in all_carriers}
            raw     = _slice_date(raw, date_range)
            grouped = {k: df for k, df in _apply_carrier_groups(raw, carrier_groups).items()
                       if not df.empty}
        elif ctype == "direct":
            df      = data.loc[date_range[0]:date_range[1]] if date_range else data.copy()
            grouped = {comp_key: df}

        if multiplier != 1.0:
            grouped = {k: df * multiplier for k, df in grouped.items()}

        for group_label, df in grouped.items():
            if aggregate_buses:
                df = df.sum(axis=1).to_frame("fleet")

            row_label  = comp_key if ctype == "direct" else f"{group_label}\n{comp_key}"
            is_per_bus = (render == "area" and not aggregate_buses)

            def _resolve_vmax(vmax_spec, bus=None):
                if isinstance(vmax_spec, (dict, pd.Series)):
                    if bus is not None:
                        return vmax_spec.get(bus)
                    return sum(vmax_spec.values()) if isinstance(vmax_spec, dict) else vmax_spec.sum()
                return vmax_spec

            if not is_per_bus:
                render_specs.append(dict(
                    label=row_label, df=df, render=render,
                    cmap=cmap, symmetric=sym, row_height=rh,
                    vmax_group=vg, unit=unit, per_bus_group=None,
                    color=color, neg_color=neg_color,
                    vmax_override=_resolve_vmax(vmax_override),
                ))
            else:
                group_id = f"{comp_key}__{group_label}"
                for bus in df.columns:
                    render_specs.append(dict(
                        label=f"{row_label}\n{bus}", df=df[[bus]],
                        render="area", cmap=cmap, symmetric=sym, row_height=rh,
                        vmax_group=vg, unit=unit, per_bus_group=group_id,
                        color=color, neg_color=neg_color,
                        vmax_override=_resolve_vmax(vmax_override, bus=bus),
                    ))

    if not render_specs:
        raise ValueError("No data to plot — check carrier_groups and components.")

    # ── Step 2: vmax per group ────────────────────────────────────────────────
    vmax_by_group = {}
    for spec in render_specs:
        vg = spec["vmax_group"]
        if vg is None or spec["df"].empty:
            continue
        vmax_by_group[vg] = max(vmax_by_group.get(vg, 0), spec["df"].abs().values.max())

    def _make_norm(spec):
        vg   = spec["vmax_group"]
        vmax = spec["vmax_override"] or vmax_by_group.get(vg) if vg else spec["vmax_override"]
        if vmax is None:
            vmax = spec["df"].abs().values.max() if not spec["df"].empty else 1.0
        return plt.Normalize(-max(vmax, 1e-6), max(vmax, 1e-6)) if spec["symmetric"] \
               else plt.Normalize(0, max(vmax, 1e-6))

    for spec in render_specs:
        spec["norm"] = _make_norm(spec)

    # ── Step 3: group into layout blocks, tag with position in group ──────────
    blocks = []
    i = 0
    while i < len(render_specs):
        gid = render_specs[i]["per_bus_group"]
        if gid is None:
            blocks.append([render_specs[i]])
            i += 1
        else:
            group = []
            while i < len(render_specs) and render_specs[i]["per_bus_group"] == gid:
                group.append(render_specs[i])
                i += 1
            blocks.append(group)

    for block in blocks:
        n = len(block)
        for idx, spec in enumerate(block):
            spec["_bus_idx"]    = idx
            spec["_block_size"] = n

    # ── Step 4: build figure with nested GridSpec ─────────────────────────────
    outer_heights = [sum(s["row_height"] for s in block) for block in blocks]
    fig      = plt.figure(figsize=(fig_width, sum(outer_heights) + 1))
    outer_gs = GridSpec(len(blocks), 1, figure=fig,
                        height_ratios=outer_heights, hspace=component_gap,
                        right=0.88)   # reserve right margin for colorbars


    axes     = []
    first_ax = None
    for block_idx, block in enumerate(blocks):
        if len(block) == 1:
            ax = fig.add_subplot(outer_gs[block_idx], sharex=first_ax)
            if first_ax is None:
                first_ax = ax
            axes.append(ax)
        else:
            inner_gs = GridSpecFromSubplotSpec(
                len(block), 1,
                subplot_spec=outer_gs[block_idx],
                hspace=0,
                height_ratios=[s["row_height"] for s in block],
            )
            for bus_idx in range(len(block)):
                ax = fig.add_subplot(inner_gs[bus_idx], sharex=first_ax)
                if first_ax is None:
                    first_ax = ax
                axes.append(ax)

    for ax in axes[:-1]:
        ax.tick_params(labelbottom=False)

    # ── Step 5: time axis ─────────────────────────────────────────────────────
    first_df     = next(s["df"] for s in render_specs if not s["df"].empty)
    time_index   = first_df.index
    month_ticks  = [i for i in range(len(time_index))
                    if i == 0 or time_index[i].month != time_index[i - 1].month]
    month_labels = [time_index[i].strftime("%b %Y") for i in month_ticks]
    # Monday at hour 0 only — marks the exact start of each week
    week_ticks   = [i for i in range(len(time_index))
                    if time_index[i].weekday() == 0 and time_index[i].hour == 0]
    if show_day_ticks:
        day_ticks  = [i for i in range(len(time_index))
                      if time_index[i].hour == 0]
        day_labels = [time_index[i].strftime("%-d") for i in day_ticks]

    # ── Step 6: render ────────────────────────────────────────────────────────
    for ax, spec in zip(axes, render_specs):
        vmax = spec["norm"].vmax
        ylim = (-vmax, vmax) if spec["symmetric"] else (0, vmax)

        if spec["render"] == "heatmap":
            _render_heatmap_row(fig, ax, spec["label"], spec["df"],
                                spec["norm"], spec["cmap"], spec["unit"],
                                fontsize=fontsize, label_gap=label_gap)
        elif spec["per_bus_group"] is not None:
            group_label, bus_name = spec["label"].rsplit("\n", 1)
            mid   = spec["_block_size"] // 2
            label = group_label if spec["_bus_idx"] == mid else ""
            _render_area_row(ax, label, spec["df"], ylim,
                             bus_name=bus_name, unit=spec["unit"],
                             color=spec["color"], neg_color=spec["neg_color"],
                             fontsize=fontsize, label_gap=label_gap)

        else:
            # single-row area (aggregate or non-grouped)
            _render_area_row(ax, spec["label"], spec["df"], ylim,
                             unit=spec["unit"],
                             color=spec["color"], neg_color=spec["neg_color"],
                             fontsize=fontsize, label_gap=label_gap)

    axes[-1].set_xticks(month_ticks)
    axes[-1].set_xticklabels(month_labels, rotation=45, ha="right", fontsize=fontsize - 1)
    axes[-1].set_xticks(week_ticks, minor=True)
    axes[-1].tick_params(axis="x", which="minor", length=4, width=0.8, labelbottom=False)
    if show_day_ticks:
        for pos, lbl in zip(day_ticks, day_labels):
            axes[-1].text(pos, -0.07, lbl,
                          transform=axes[-1].get_xaxis_transform(),
                          ha="center", va="top", fontsize=6, clip_on=False)
    if title:
        fig.suptitle(title, fontsize=11)

    return fig, axes