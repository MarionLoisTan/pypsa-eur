# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2023-2025 The PyPSA-Eur Authors, Aleksander Grochowicz
#
# SPDX-License-Identifier: MIT

# Based on: https://gist.github.com/fneum/47b857862dd9148a22eca5a2e85caa9a

if "snakemake" in globals():
    filename = snakemake.output[0]
else:
    filename = "weather_scenarios.yaml"

import itertools

template = """
weather_year_{year}:
  snapshots:
    start: "{year}-07-01 00:00"
    end: "{end_year}-06-30 23:00"
    inclusive: "both"
  atlite:
    default_cutout: europe-1941-1960-era5
    cutouts:
      europe-1941-1960-era5:
        module: era5
        x: [-12., 35.]
        y: [33., 72]
        dx: 0.25
        dy: 0.25
        time: ['{year}', '{end_year}']
"""

template_steps = """
weather_year_{year}:
  snapshots:
    start: "{year}-07-01 00:00"
    end: "{end_year}-06-30 23:00"
    inclusive: "both"
  atlite:
    default_cutout: europe-{step_start}-{step_end}-era5
    cutouts:
      europe-{step_start}-{step_end}-era5:
        module: era5
        x: [-12., 35.]
        y: [33., 72]
        dx: 0.25
        dy: 0.25
        time: ['{year}', '{end_year}']
  renewable:
    onwind:
      cutout: europe-{step_start}-{step_end}-era5
    offwind-ac:
      cutout: europe-{step_start}-{step_end}-era5
    offwind-dc:
      cutout: europe-{step_start}-{step_end}-era5
    offwind-float:
      cutout: europe-{step_start}-{step_end}-era5
    solar:
      cutout: europe-{step_start}-{step_end}-era5
    solar-hsat:
      cutout: europe-{step_start}-{step_end}-era5
    hydro:
      cutout: europe-{step_start}-{step_end}-era5
  solar_thermal:
    cutout: europe-{step_start}-{step_end}-era5
  sector:
    heat_demand_cutout: europe-{step_start}-{step_end}-era5
  lines:
    dynamic_line_rating:
      cutout: europe-{step_start}-{step_end}-era5
"""

first_year = 1941
last_year = 2021

# Counterintuitive but this is what works.
config_values = dict(year=range(first_year,last_year + 1), end_year = range(first_year + 1, last_year + 1))

combinations = [
      dict(zip(config_values.keys(), values))
      for values in itertools.product(*config_values.values())
      if values[0] +1 == values[1]
  ]


steps = True
step_size = 5

if steps:
  combinations_fys = []
  for i, config in enumerate(combinations):
      # Assign config["year"] to the right step.
      if config["year"] - first_year < step_size:
          step = 0
      else:
          step = (config["year"] - first_year) // step_size
      config["step_start"] = step * step_size + first_year
      config["step_end"] = config["step_start"] + step_size
      if config["step_end"] > last_year:
          config["step_end"] = last_year
      combinations_fys.append(config)
  with open(filename, "w") as f:
      for i, config in enumerate(combinations_fys):
          f.write(template_steps.format(**config))
else:
  with open(filename, "w") as f:
      for i, config in enumerate(combinations):
          f.write(template.format(scenario_number=i, **config))