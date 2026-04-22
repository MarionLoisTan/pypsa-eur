# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2023-2025 The PyPSA-Eur Authors, Aleksander Grochowicz
#
# SPDX-License-Identifier: MIT

# Based on: https://gist.github.com/fneum/47b857862dd9148a22eca5a2e85caa9a

if "snakemake" in globals():
    filename = snakemake.output[0]
else:
    # filename = "scen.weather_years+dmg.yaml"
    filename = "scen.summer_years+dmg.yaml"

# --- Configuration -----------------------------------------------------------
CUTOUT_NAME = "europe-2018-2023-era5_fg10_lmlt"
first_year = 2018
last_year = 2022  # last scenario spans last_year → last_year+1

# --- Templates ---------------------------------------------------------------

template_cap = """
weather_year_{year}_dmg_cap:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{end_year}-05-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout}:
        time:
        - '{year}'
        - '{end_year}'
  renewable:
    onwind:
      cutout: {cutout}
    offwind-ac:
      cutout: {cutout}
    offwind-dc:
      cutout: {cutout}
    offwind-float:
      cutout: {cutout}
    solar:
      cutout: {cutout}
    solar-hsat:
      cutout: {cutout}
    hydro:
      cutout: {cutout}
  solar_thermal:
    cutout: {cutout}
  lines:
    dynamic_line_rating:
      cutout: {cutout}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc, offwind-float]
  damage:
    nuclear: capacity
"""

template_dis = """
weather_year_{year}_dmg_dis:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{end_year}-05-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout}:
        time:
        - '{year}'
        - '{end_year}'
  renewable:
    onwind:
      cutout: {cutout}
    offwind-ac:
      cutout: {cutout}
    offwind-dc:
      cutout: {cutout}
    offwind-float:
      cutout: {cutout}
    solar:
      cutout: {cutout}
    solar-hsat:
      cutout: {cutout}
    hydro:
      cutout: {cutout}
  solar_thermal:
    cutout: {cutout}
  lines:
    dynamic_line_rating:
      cutout: {cutout}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc, offwind-float]
  damage:
    nuclear: dispatch
"""

template_cap_summer = """
weather_year_{year}_dmg_cap:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{year}-08-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout}:
        time:
        - '{year}'

  renewable:
    onwind:
      cutout: {cutout}
    offwind-ac:
      cutout: {cutout}
    offwind-dc:
      cutout: {cutout}
    offwind-float:
      cutout: {cutout}
    solar:
      cutout: {cutout}
    solar-hsat:
      cutout: {cutout}
    hydro:
      cutout: {cutout}
  solar_thermal:
    cutout: {cutout}
  lines:
    dynamic_line_rating:
      cutout: {cutout}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc, offwind-float]
  damage:
    nuclear: capacity
"""

template_dis_summer = """
weather_year_{year}_dmg_dis:
  snapshots:
    start: "{year}-06-01 00:00"
    end: "{year}-08-31 23:00"
    inclusive: both
  atlite:
    cutouts:
      {cutout}:
        time:
        - '{year}'

  renewable:
    onwind:
      cutout: {cutout}
    offwind-ac:
      cutout: {cutout}
    offwind-dc:
      cutout: {cutout}
    offwind-float:
      cutout: {cutout}
    solar:
      cutout: {cutout}
    solar-hsat:
      cutout: {cutout}
    hydro:
      cutout: {cutout}
  solar_thermal:
    cutout: {cutout}
  lines:
    dynamic_line_rating:
      cutout: {cutout}
  electricity:
    renewable_carriers: [solar, solar-hsat, onwind, offwind-ac, offwind-dc, offwind-float]
  damage:
    nuclear: dispatch
"""

# --- Generate ----------------------------------------------------------------

with open(filename, "w") as f:
    for year in range(first_year, last_year + 1):
        end_year = year + 1
        f.write(template_cap_summer.format(year=year, end_year=end_year, cutout=CUTOUT_NAME))
        f.write(template_dis_summer.format(year=year, end_year=end_year, cutout=CUTOUT_NAME))
