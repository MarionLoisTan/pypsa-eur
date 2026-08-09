# PyPSA-Eur — Climate Damage Extension

This repository is a fork of [PyPSA-Eur](https://github.com/PyPSA/pypsa-eur) extended with a weather-damage modelling framework. The extension adds time-varying technology damage profiles derived from ERA5 weather data and applies them to PyPSA networks during capacity planning and/or dispatch re-optimisation.

Two damage mechanisms are currently implemented:

- **Nuclear - water temperature damage** — derating and shutdown of nuclear generators driven by river/lake surface temperatures exceeding cooling efficiency and shutdown thresholds
- **Wind - extreme wind speeds damage** — derating of wind generators (onshore and offshore) driven by extreme 10 m wind gust speeds

---

## New scripts

All damage-related scripts live in `scripts/build_damage_profiles/`.

| Script | Purpose |
|--------|---------|
| `build_damage_cutout_smk.py` | Snakemake wrapper: copies an existing atlite cutout and prepares additional ERA5 features required for damage calculations (`lake_s_temp`, `wnd_gust10m`) |
| `build_nuclear_damage_profiles.py` | Builds hourly nuclear damage profiles at bus level. Uses lake surface temperature and a vulnerability and discahrge regulations table from [Luo et al., 2023](https://www.nature.com/articles/s43247-023-00782-w). Outputs `profile` (time × bus, values ∈ [0,1]) and per-plant diagnostics |
| `build_wind_damage_profiles.py` | Builds hourly wind damage profiles at bus level. Uses 10 m wind gust speed and layout-weighted spatial aggregation consistent with `build_renewable_profiles.py`. Implements the damage fraction function from [Hong and Möller](https://www.sciencedirect.com/science/article/pii/S0960148112000213) |
| `_apply.py` | Shared utility called by `prepare_network.py` and `solve_operations_network.py`. Reads damage profiles from Snakemake input, resamples if needed, and multiplies into `n.generators_t.p_max_pu` |
| `water_temperature_regulations.csv` | Lookup table: degrees below shutdown water temperature → inoperable fraction due to regulatory discharge constraints |
| `water_temperature_vulnerability.csv` | Lookup table: effective temperature above desired water temperature → vulnerability-based damage fraction |

### Snakemake rules (`rules/damage.smk`)

| Rule | Purpose |
|------|---------|
| `build_damage_cutout` | Prepare a feature-suffixed copy of a cutout |
| `build_all_damage_cutouts` | Convenience target for all cutouts in `damage_config.yaml` |
| `build_nuclear_damage_profile` | Bus-level nuclear damage profile for a given cluster count |
| `build_nuclear_plant_damage_profile` | Per-plant diagnostic nuclear damage profile |
| `build_all_nuclear_damage_profiles` | Convenience target across all cluster values |
| `build_wind_damage_profile` | Bus-level wind damage profile for a given carrier and cluster count |
| `build_all_wind_damage_profiles` | Convenience target for all wind carriers |
| `solve_all_scenarios` | Solve capacity + dispatch for all scenarios in the scenarios file |
| `solve_all_base_dispatch` | Dispatch re-solve for undamaged base scenarios only |
| `solve_all_damaged_dispatch` | Dispatch re-solve for scenarios with dispatch-phase damage |

### Damage application phases

Damage can be applied at two phases, controlled per-technology in the scenario config:

- `capacity` — applied in `prepare_network.py`; affects capacity optimisation
- `dispatch` — applied in `solve_operations_network.py`; affects dispatch re-optimisation only

---

## Configuration

Damage parameters are stored in `config/damage_config.yaml`. Scenario-level damage is enabled per-technology in the scenarios YAML under a `damage:` key, e.g.:

```yaml
damage:
  nuclear: dispatch
  onwind: capacity
```

Available configs in this repository:

| Config file | Study |
|-------------|-------|
| `config/config.2021-GB+dmg_win.yaml` | GB 2021 — wind |
| `config/config.2022-FR+dmg_nucl.yaml` | FR 2022 — nuclear |
| `config/config.10years-FR+dmg_nucl.yaml` | FR multi-year — nuclear, for comparing with ENTSO-E outages|

---

## Running on SOPHIA

Two cluster submission wrappers are available:

| Wrapper | Partition(s) | CPUs | Use for |
|---------|-------------|------|---------|
| `snakemake_cluster_preparations` | `workq`, `rome`, `fatq` | `{threads}` (rule-defined) | Data preparation: cutout building, profile generation, `prepare_network` |
| `snakemake_cluster_thin` | `fatq`, `rome`, `gpuq`, `workq` | 32 (exclusive node) | Network solving: `solve_network`, `solve_operations_network` |

> Remove the `-n` (dry-run) flag from any command below before submitting for real execution.

---

### Step 1 — Prepare networks (`snakemake_cluster_preparations`)

Run `--until prepare_network` to build damage cutouts, damage profiles, and prepared networks before solving.

**GB 2021 wind damage:**
```bash
./snakemake_cluster_preparations solve_all_base_dispatch \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --until prepare_network \
  --jobs 5 -n

./snakemake_cluster_preparations solve_all_scenarios \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --jobs 5 \
  --until prepare_network \
  --forcerun prepare_network \
  -n
```

**FR 2022 nuclear damage:**
```bash
./snakemake_cluster_preparations solve_all_base_dispatch \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --until prepare_network \
  --jobs 5 -n

./snakemake_cluster_preparations solve_all_scenarios \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --jobs 5 \
  --until prepare_network \
  --forcerun prepare_network \
  -n
```

**FR 10-year nuclear damage:**
```bash
./snakemake_cluster_preparations solve_all_base_dispatch \
  --configfile config/config.10years-FR+dmg_nucl.yaml \
  --until prepare_network \
  --jobs 5 -n

./snakemake_cluster_preparations solve_all_scenarios \
  --configfile config/config.10years-FR+dmg_nucl.yaml \
  --jobs 10 \
  --until prepare_network \
  -n
```

---

### Step 2 — Solve networks (`snakemake_cluster_thin`)

**GB 2021 wind damage:**
```bash
# Solve all scenarios
./snakemake_cluster_thin solve_all_scenarios \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --jobs 3 -n

# Solve scenarios and base dispatch together
./snakemake_cluster_thin solve_all_scenarios solve_all_base_dispatch \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --jobs 3 -n

# Force re-solve
./snakemake_cluster_thin solve_all_scenarios solve_all_base_dispatch \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --jobs 3 \
  --forcerun solve_network \
  -n

# Single network target
./snakemake_cluster_thin \
  results/2021-GB/weather_year_2021_win_cap_LGLP/networks/base_s_5_elec__op.nc \
  --configfile config/config.2021-GB+dmg_win.yaml \
  --jobs 1 -n
```

**FR 2022 nuclear damage:**
```bash
# Solve all scenarios and base dispatch
./snakemake_cluster_thin solve_all_scenarios solve_all_base_dispatch \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --jobs 3 -n

# Force re-solve
./snakemake_cluster_thin solve_all_scenarios solve_all_base_dispatch \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --jobs 3 \
  --forcerun solve_network \
  -n

# Solve base dispatch only
./snakemake_cluster_thin solve_all_base_dispatch \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --jobs 3 -n

# Single network target
./snakemake_cluster_thin \
  results/2022-FR/weather_year_2022_nuc_cap/networks/base_s_5_elec__op.nc \
  --configfile config/config.2022-FR+dmg_nucl.yaml \
  --jobs 1 -n
```

**FR 10-year nuclear damage:**
```bash
# Solve all scenarios and base dispatch
./snakemake_cluster_thin solve_all_scenarios solve_all_base_dispatch \
  --configfile config/config.10years-FR+dmg_nucl.yaml \
  --jobs 3 -n

# Solve all scenarios only
./snakemake_cluster_thin solve_all_scenarios \
  --configfile config/config.10years-FR+dmg_nucl.yaml \
  --jobs 3 -n

# Solve base dispatch only
./snakemake_cluster_thin solve_all_base_dispatch \
  --configfile config/config.10years-FR+dmg_nucl.yaml \
  --jobs 5 -n
```