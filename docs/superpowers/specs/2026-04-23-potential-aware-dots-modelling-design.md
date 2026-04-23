# Potential-aware DOTS modelling — design

**Date:** 2026-04-23
**Status:** Draft — pending user review
**Author:** n.tonthat + Claude Code (brainstorming)

## Goal

Improve the powerlifting progress model by introducing **innate-potential signals** (starting DOTS + rolling career-so-far features + tier labels) and migrating the modelling target framework to DOTS. The intuition: a lifter's first few comps contain strong signal about their ceiling. A lifter who opens at 400 DOTS is on a different trajectory from one who hovers at 300 after three comps, and the current model has no way to encode that.

Current state: v7 R² = 0.351, v8 in progress (target changed to `pct_change_total`). Journey tracked in `data_science_journey.md`.

## Scope

Three staged iterations landed sequentially. Each produces a journey-doc entry and becomes the baseline for the next.

| Version | Hypothesis | Target |
|---------|-----------|--------|
| **v9**  | Cleaner data + DOTS as features improves R² without changing target | `pct_change_total` |
| **v10** | Innate-potential features (first-comp, rolling, tiers) capture headroom-to-ceiling | `pct_change_total` |
| **v11** | DOTS-based target is more honest; dual-head comparison settles which framing is better | `pct_change_total` **and** `pct_change_dots` |

Out of scope: multi-output XGBoost, removing Wilks, long-horizon (N-year) predictions, sequence models, full CI for training.

## Architecture

### Files touched per version

| File | v9 | v10 | v11 |
|------|----|-----|-----|
| `steps/03_raw.py` | bomb-out filter, total-consistency filter, bw-validity filter, DOTS column (fallback if not in source) | — | — |
| `steps/03_base.py` | DOTS-parallel features (`previous_dots`, `pct_change_dots`, etc.) | first-comp / rolling / tier feature functions | — |
| `steps/05_train.py` | add DOTS features to `modelling_cols` | add potential + tier features to `modelling_cols`, per-tier slice eval | two-target loop, dual Optuna studies, normalised-RMSE comparison |
| `steps/conf.py` | `DOTS_VALID_BW_RANGE`, filter tolerances | `DOTS_TIERS` mapping, helper to assign tier | — |
| `tests/` | new tests per section below | new tests | new tests |
| `about.py` / `pages/` | — | — | DOTS trajectory chart, prediction-head toggle |

### Key cross-cutting decisions

- Wilks stays through v11 as both a feature and dashboard column. Removing Wilks is separate cleanup.
- Tier cutoffs live in `conf.py` as the single source of truth for feature engineering, evaluation slices, and dashboard labels.
- All changes are accumulative: v10 keeps v9's features, v11 keeps v10's features.
- Temporal split (`TEMPORAL_TRAIN_CUTOFF = 0.67`, `TEMPORAL_VAL_CUTOFF = 0.75`) and Optuna budget (500 trials, `MedianPruner(n_warmup_steps=50)`) stay identical across versions so ablations are apples-to-apples.

## DOTS reference

**Formula origin:** Tim Konertz (BVDK / German Powerlifting Federation), 2019. Reference implementation in OpenPowerlifting's `opl-csv` source tree at `crates/coefficients/src/dots.rs`. Fifth-degree polynomial in denominator; multiplied against total (kg) and scaled by 500.

**Valid bodyweight ranges (Konertz):**
- Men: 40 – 210 kg
- Women: 40 – 150 kg

Rows with bodyweight outside this range are filtered out in v9 (DOTS is mathematically undefined outside the calibration window).

**Classification tiers (community convention):**

| DOTS range | Tier |
|------------|------|
| < 200 | Beginner |
| 200 – 300 | Intermediate |
| 300 – 400 | Advanced |
| 400 – 500 | Elite |
| 500+ | World Class |

Sex-agnostic (DOTS is normalised so M/F elite scores converge). Half-open intervals: the lower bound is inclusive, upper exclusive. Cutoffs are community convention (LiftVault / Iron Compare / Zylo), not an official federation standard.

**References:**
- OpenPowerlifting bulk-csv documentation: https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html
- LiftVault DOTS calculator and tier reference: https://liftvault.com/resources/powerlifting-calculator/
- Iron Compare DOTS calculator: https://ironcompare.com/tools/dots-calculator
- History of powerlifting scoring (Wilks to DOTS): https://dotscalculator.com/blog/history-of-powerlifting-score-wilks-to-dots

### `conf.py` additions

```python
DOTS_VALID_BW_RANGE = {"M": (40.0, 210.0), "F": (40.0, 150.0)}

# Community convention; sex-agnostic. Half-open intervals.
DOTS_TIERS = {
    "Beginner":     (0.0,   200.0),
    "Intermediate": (200.0, 300.0),
    "Advanced":     (300.0, 400.0),
    "Elite":        (400.0, 500.0),
    "World Class":  (500.0, float("inf")),
}

# Ordinal encoding for model features — order reflects strength progression.
DOTS_TIER_ORDER = ["Beginner", "Intermediate", "Advanced", "Elite", "World Class"]

# Data quality tolerances
TOTAL_CONSISTENCY_TOLERANCE_KG = 2.5
```

## v9 — Data cleaning + DOTS features

### Data cleaning (in `03_raw.py`)

Three new filters, each with before/after row-count logging:

1. **Bomb-out filter:** drop rows where `squat <= 0 OR bench <= 0 OR deadlift <= 0`. These are SBD events where the lifter failed all attempts on at least one lift — the `total` is recorded but progress signal is garbage.
2. **Total consistency filter:** drop rows where `abs((squat + bench + deadlift) - total) > TOTAL_CONSISTENCY_TOLERANCE_KG` (2.5 kg). Catches data-entry errors.
3. **Bodyweight-validity filter:** drop rows where `bodyweight < 40` OR (sex == "M" AND bodyweight > 210) OR (sex == "F" AND bodyweight > 150). Expected drop < 0.1%.

### DOTS integration

1. **Source check:** inspect the raw parquet schema after extraction. OpenPowerlifting publishes a `Dots` column. If present, use directly (rename to `dots` in the snake_case convention). If absent, compute via the Konertz formula in `03_raw.py` using sex-specific polynomial coefficients in `conf.py`.
2. **Base-layer DOTS features** (parallel to existing Wilks features in `03_base.py`):
   - `previous_dots` — `pl.col("dots").shift(1).over("primary_key")`
   - `dots_progress` — `(dots - previous_dots) / time_since_last_comp_years`
   - `pct_change_dots` — `(dots - previous_dots) / previous_dots * 100`, null when `previous_dots <= 0`
   - `prev_pct_change_dots` — `pct_change_dots.shift(1).over("primary_key")`
   - `rolling_avg_dots_3`, `prev_rolling_avg_dots_3`
   - `prev_dots_personal_best`, `prev_dots_vs_pb`, `prev_distance_from_pb_dots`
3. **Guard:** `pct_change_dots` gets the same short-gap null-out as `pct_change_total` (null when `time_since_last_comp_days < MIN_DAYS_BETWEEN_COMPS`).

### `05_train.py` changes

Add to `modelling_cols`: `previous_dots`, `prev_pct_change_dots`, `prev_dots_vs_pb`, `prev_distance_from_pb_dots`, `prev_rolling_avg_dots_3`. Keep existing Wilks features. Target unchanged (`pct_change_total`).

### v9 validation

**Unit tests (new `tests/` directory):**
- `test_bombout_filter_drops_rows_with_zero_lifts`
- `test_total_consistency_filter_kept_within_tolerance`
- `test_total_consistency_filter_drops_outside_tolerance`
- `test_bodyweight_validity_filter_male_range`
- `test_bodyweight_validity_filter_female_range`
- `test_dots_formula_matches_openpowerlifting_reference` (if computed locally; compare ≥10 known rows)
- `test_previous_dots_no_leakage` (synthetic 3-lifter × 5-comp fixture)
- `test_pct_change_dots_null_when_previous_zero`

**Ablation:** v9 vs v8 on identical temporal split. Metrics: test R², RMSE, MAE, Median AE, baseline RMSE, per-sex R², top weight-class R².

**Data-quality checkpoints logged to journey doc and MLflow tags:**
- Rows before/after each new filter (absolute and %).
- DOTS mean/std per sex / weight class vs. published norms (OpenPowerlifting rankings page as sanity anchor).

**Journey doc:** new `## v9: Data cleaning + DOTS features` section following the existing 8-column table pattern.

## v10 — Potential features + tier labels

Four feature groups in `03_base.py`, all leak-free. All use DOTS, not Wilks.

### Group A — Static first-comp birth certificate

Computed once per lifter from their first competition, broadcast as constant columns:

- `first_comp_dots` — DOTS at comp 1
- `first_comp_total_per_bw` — total / bw at comp 1
- `first_comp_age` — approx_age at comp 1
- `first_comp_percentile_vs_sex_wc_tenure0` — first-comp DOTS percentile vs. other lifters' first-comp DOTS within the same sex × IPF weight class

Implementation:
```python
df.with_columns(
    pl.col("dots").first().over("primary_key").alias("first_comp_dots"),
    # ...
)
```

### Group B — Rolling career-so-far

At comp k, every feature uses only data from comps 1..k-1 (strict — current comp excluded). All features are null at comp 1 (no prior data).

- `max_dots_so_far` — `pl.col("previous_dots").cum_max().over("primary_key")` — `previous_dots` is already shifted by 1, so cum_max is over comps 1..k-1.
- `best_growth_rate_so_far` — `pl.col("pct_change_dots").shift(1).cum_max().over("primary_key")` — max of `pct_change_dots` values from comps 2..k-1 (comp 1 has null pct_change). Null until comp 3.
- `dots_growth_trend` — slope of linear fit through `(previous_dots, date)` pairs over `primary_key`, i.e. across comps 1..k-1. Positive = improving; near-zero = plateau. Null until comp 3 (need ≥2 points for a slope).
- `early_growth_rate_dots_per_year` — `(dots at 3rd comp − dots at 1st comp) / years_between`, broadcast as constant **from comp 4 onward** (uses comps 1..3, available to any row where `cumulative_comps >= 4`). Null for comps 1–3. **Critical**: at comp 3 this feature must be null — using comp 3's own DOTS would leak into the `pct_change_total` target for that row.
- `comps_above_tier_threshold_so_far` — count of prior comps (shift(1) applied) crossing each tier lower bound. One column per tier (e.g., `comps_above_elite_so_far`). Tells the model "they've hit Elite 4 times already" vs "one-time hit".

### Group C — Tier labels

Using `conf.DOTS_TIERS` (community convention, sex-agnostic):

- `starting_tier` — tier at first comp (constant per lifter, ordinal-encoded 0–4 via `DOTS_TIER_ORDER`)
- `prev_tier` — tier at comp k-1
- `max_tier_so_far` — highest tier crossed in prior comps
- `tiers_climbed_so_far` — `max_tier_so_far - starting_tier` (discrete progression signal)

### Group D — Potential × context interactions

Cheap interactions, let XGBoost decide if useful:

- `starting_tier × years_competing` — does a strong starter still have growth left after N years?
- `max_tier_so_far × time_since_last_comp_years` — has a high-ceiling lifter been dormant?

### `05_train.py` changes

Add all Group A/B/C/D features to `modelling_cols`. Add per-starting-tier R²/RMSE slice evaluation block (mirroring the existing per-sex and per-weight-class blocks around `05_train.py:450`). Target unchanged.

### v10 validation

**Unit tests:**
- `test_first_comp_features_constant_per_lifter` — `first_comp_dots.n_unique() == 1` per primary_key
- `test_max_dots_so_far_uses_only_prior_comps` — value at comp k == `max(dots[0..k-1])`; null at comp 1
- `test_tier_cutoffs_half_open_at_200` — DOTS exactly 200.0 → Intermediate, not Beginner
- `test_tier_cutoffs_half_open_at_500` — DOTS exactly 500.0 → World Class
- `test_tiers_climbed_non_negative` — no lifter has `tiers_climbed_so_far < 0`
- `test_starting_tier_matches_first_comp_dots` — spot-check derivation on fixture
- `test_early_growth_rate_null_before_comp_4` — uses comps 1..3, so first non-null at comp 4
- `test_early_growth_rate_uses_no_current_comp_dots` — at comp 4, feature value depends only on dots at comps 1..3, not comp 4 itself

**Ablation:** v10 vs v9. Expect the largest lift of the three versions here (this is where the main hypothesis is tested).

**Per-starting-tier slice evaluation** — new section in the journey doc. Break test R²/RMSE down by the 5 tiers. Hypothesis to record: *largest R² gain concentrated in Beginner/Intermediate (high variance in growth), smallest in Elite/World Class (near ceiling).* If this hypothesis fails, the journey doc records it honestly.

**Data-quality checkpoint:** lifter count per community tier at first comp; tier transition matrix (start tier → max-achieved tier). If any tier has <50 lifters, flag in the journey doc and consider merging bins or adding a supplementary quintile-based feature in a follow-up.

**Feature importance sanity:** at least one Group B feature (rolling career-so-far) should appear in top 15 gain. If none do, the rolling-potential thesis is weaker than hypothesised — record honestly.

## v11 — Dual-head training

Two heads trained on the **exact same feature set and temporal split**. Only the target differs.

### Training structure

Replace the current single-target block in `05_train.py` with a two-target loop:

```python
for TARGET in ["pct_change_total", "pct_change_dots"]:
    run_name = f"{TARGET}_v11"
    with mlflow.start_run(run_name=run_name):
        # existing Optuna + retrain + evaluate flow
        # target-specific winsorization (each head computes own p1/p99)
```

Two parent MLflow runs per pipeline invocation, filterable by `target` tag.

### Comparison metrics (logged to journey doc)

Same-scale metrics are not directly comparable across heads. Add:

- **Normalised RMSE:** `test_rmse / y_test.std()` per head — scale-free, directly comparable.
- **Spearman rank correlation (ρ):** between predictions and ground truth per head — scale-free, measures ranking quality (does the model correctly order lifters from most- to least-progress?).
- **Per-starting-tier breakdown for both heads.**

### Streamlit dashboard changes

Minimum required by this work (nothing speculative):

- `pages/02_user_data.py` — add DOTS trajectory chart alongside existing Wilks chart.
- `pages/03_strength_progress.py` — add DOTS prediction row/toggle alongside `pct_change_total` predictions.
- New artifact: `s3://powerlifting-ml-progress/base/model_predictions_v11.parquet` with columns `[primary_key, date, pred_pct_change_total, pred_pct_change_dots, starting_tier]`. Dashboard reads this directly rather than loading the model.

### v11 validation

**Unit tests:**
- `test_both_heads_trained_on_same_features` — `list(X_train.columns)` identical between studies
- `test_both_heads_share_temporal_split` — `train_mask.sum()`, `val_mask.sum()`, `test_mask.sum()` identical
- `test_normalised_rmse_computation` — synthetic data with known std, verify formula
- `test_predictions_parquet_schema` — required columns present

**Ablation:** v11 kg-head vs v10. Expect neutral or minor improvement (kg head is essentially v10 rerun).

**Head-to-head comparison** — primary question of v11. Recommend primary head for dashboard based on normalised-RMSE and Spearman ρ.

**Dashboard smoke test:** manual checklist in the journey doc. `uv run streamlit run about.py`, navigate 02 → 03 → 04, confirm DOTS columns render, no stale-cache issues.

## Validation plan — cross-cutting

### Global leakage guard

Single parametrised pytest that, for every `prev_*` / `first_comp_*` / `*_so_far` feature, asserts the value at comp k uses only data from comps 1..k-1 (or comp 1 only for `first_comp_*`). Synthetic 3-lifter × 5-comp fixture with known ground-truth feature values. Runs every version; prevents the class of bug v5 corrected (see journey doc).

### Ablation protocol (per version)

- Same temporal split constants (already in `conf.py`).
- Same Optuna search space, 500 trials, `MedianPruner(n_warmup_steps=50)`.
- Standard 8-column journey-doc table row: version, R², RMSE, MAE, Median AE, Baseline RMSE, RMSE Lift, Features, Trials.
- Per-segment breakdown (sex + weight class) for every version; add per-tier starting from v10.

### Regression guards

- Final per-version test run fails loudly if test R² drops by >0.02 vs. prior version. Forces an explicit "why" in the journey doc rather than silent acceptance.
- Feature-importance delta logged: features that were top-15 in the prior version but fell out are flagged.

### Non-goals for validation

- Full CI integration for model training (just pytest in CI; training stays manual).
- Cross-validation (temporal split is the truth; k-fold would leak).
- Backtesting against historical Streamlit predictions.

## Risks and mitigations

| Risk | Mitigation |
|------|-----------|
| Community tier cutoffs leave one bin near-empty (e.g., <1% World Class) → useless as a feature | v10 data-quality checkpoint logs distribution. If any bin <50 lifters, revisit by merging bins or adding quintile feature. |
| DOTS target has different noise characteristics than `pct_change_total` → one head artificially looks better | Normalised RMSE + Spearman ρ both computed; head-to-head takes both into account. |
| Potential features don't actually help — R² stays flat | v10 journey-doc entry records this honestly. v11 still proceeds (DOTS target reframing is independently valuable). |
| DOTS formula discrepancy if we compute locally vs. OpenPowerlifting's published `Dots` column | Prefer source column when available; unit test compares ≥10 known rows when computing locally. |
| Bomb-out filter drops more than expected (>5%) and reshapes dataset | Log drop counts per filter. Journey doc v9 entry records absolute and percentage drops so future versions can sanity-check. |

## Open questions deferred to implementation

These are intentionally not resolved in this design — they require inspecting real data to answer:

1. Is the `Dots` column present in the OpenPowerlifting CSV as published? (If yes, skip the local computation; if no, add the formula to `03_raw.py`.)
2. What is the actual distribution of lifters across the 5 community tiers in our dataset? Answered in v10 data-quality checkpoint.
3. Does `early_growth_rate_dots_per_year` (requires 3 comps) force us to change `MIN_COMPETITIONS` from 3 to 4? Answered during v10 implementation.

## Transition

After user review, next step is invoking the `superpowers:writing-plans` skill to produce a detailed implementation plan covering v9, v10, v11 as distinct execution phases.
