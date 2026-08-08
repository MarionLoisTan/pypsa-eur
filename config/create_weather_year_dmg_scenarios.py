# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2023-2025 The PyPSA-Eur Authors, Aleksander Grochowicz
#
# SPDX-License-Identifier: MIT

# Based on: https://gist.github.com/fneum/47b857862dd9148a22eca5a2e85caa9a

if "snakemake" in globals():
    filename = snakemake.output[0]
else:
    filename = "scen.weather_years+dmg_nucl.yaml"
    # filename = "scen.summer_years+dmg.yaml"

# --- Configuration -----------------------------------------------------------
CUTOUT_TEMPLATE = "europe-{year}-era5_fg10_lmlt"
first_year = 2014
last_year = 2023  # last scenario spans last_year → last_year+1

SUMMER_MODE = False  # True: Jun–Aug snapshots; False: Jun–May (full year)

# Each key becomes the scenario name suffix; the value maps carrier → damage mode.
# Multiple carriers in one entry produce a single scenario with multiple damage lines.
DAMAGE_CONFIGS = {
    "nuc_cap": {"nuclear": "capacity"},
    "nuc_dis": {"nuclear": "dispatch"},
    # "win_cap": {"onwind": "capacity", "offwind-ac": "capacity", "offwind-dc": "capacity", "offwind-float": "capacity"},
    # "win_dis": {"onwind": "dispatch", "offwind-ac": "dispatch", "offwind-dc": "dispatch", "offwind-float": "dispatch"},
    # examples of further combinations:
    # "onwind_cap": {"onwind": "capacity"},
    # "nuc_cap_onwind_cap": {"nuclear": "capacity", "onwind": "capacity"},
}


# --- Helper ------------------------------------------------------------------

def format_damage(damage_dict):
    lines = ["  damage:"]
    for carrier, mode in damage_dict.items():
        lines.append(f"    {carrier}: {mode}")
    return "\n".join(lines)


# --- Templates ---------------------------------------------------------------

TEMPLATE_FULL = """
weather_year_{year}_{suffix}:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{end_year}-05-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout_a}:
        time:
        - '{year}'
        - '{year}'
      {cutout_b}:
        time:
        - '{end_year}'
        - '{end_year}'
  renewable:
    onwind:
      cutout:
      - {cutout_a}
      - {cutout_b}
    offwind-ac:
      cutout:
      - {cutout_a}
      - {cutout_b}
    offwind-dc:
      cutout:
      - {cutout_a}
      - {cutout_b}
    offwind-float:
      cutout:
      - {cutout_a}
      - {cutout_b}
    solar:
      cutout:
      - {cutout_a}
      - {cutout_b}
    solar-hsat:
      cutout:
      - {cutout_a}
      - {cutout_b}
    hydro:
      cutout:
      - {cutout_a}
      - {cutout_b}
  solar_thermal:
    cutout:
    - {cutout_a}
    - {cutout_b}
  lines:
    dynamic_line_rating:
      cutout:
      - {cutout_a}
      - {cutout_b}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc]
{damage_block}
"""

TEMPLATE_SUMMER = """
weather_year_{year}_{suffix}:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{year}-08-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout_a}:
        time:
        - '{year}'
        - '{year}'
  renewable:
    onwind:
      cutout: {cutout_a}
    offwind-ac:
      cutout: {cutout_a}
    offwind-dc:
      cutout: {cutout_a}
    offwind-float:
      cutout: {cutout_a}
    solar:
      cutout: {cutout_a}
    solar-hsat:
      cutout: {cutout_a}
    hydro:
      cutout: {cutout_a}
  solar_thermal:
    cutout: {cutout_a}
  lines:
    dynamic_line_rating:
      cutout: {cutout_a}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc]
{damage_block}
"""

# --- Generate ----------------------------------------------------------------

template = TEMPLATE_SUMMER if SUMMER_MODE else TEMPLATE_FULL

with open(filename, "w") as f:
    for year in range(first_year, last_year + 1):
        end_year = year + 1
        cutout_a = CUTOUT_TEMPLATE.format(year=year)
        cutout_b = CUTOUT_TEMPLATE.format(year=end_year)
        for suffix, damage_dict in DAMAGE_CONFIGS.items():
            damage_block = format_damage(damage_dict)
            f.write(template.format(
                year=year,
                end_year=end_year,
                suffix=suffix,
                cutout_a=cutout_a,
                cutout_b=cutout_b,
                damage_block=damage_block,
            ))
