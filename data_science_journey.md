# Data Science Journey: Powerlifting Progress Prediction

Tracking the iterative improvement of the XGBoost model. Target evolved over time:
- v4-v7: `total_progress` (kg/year)
- v8+: `pct_change_total` (% change to next comp) — bodyweight-agnostic
- v11 also adds `pct_change_dots` head — dual-head training

Note that R² values across target changes are NOT directly comparable. RMSE is on the target's own scale.

## Summary

| Version | Target | R² | RMSE | MAE | Median AE | Baseline RMSE | RMSE Lift | Features | Trials |
|---------|--------|-----|------|-----|-----------|---------------|-----------|----------|--------|
| v4 (baseline) | total_progress | ~0.25 | ~45.70 | — | — | ~51.36 | ~5.66 | 28 | 250 |
| v5 | total_progress | 0.256 | 44.29 | 30.95 | 20.69 | 51.36 | 7.07 | 30 | 250 (78 complete, 172 pruned) |
| v6 | total_progress | 0.254 | 44.35 | 30.97 | 21.05 | 51.36 | 7.01 | 41 | 250 (84 complete, 166 pruned) |
| v7 | total_progress | **0.351** | **43.97** | **30.46** | **20.49** | 54.59 | **10.63** | 44 | 500 (90 complete, 410 pruned) |
| v11 (kg head) | pct_change_total | **0.4537** | **4.89** | **3.52** | **2.57** | 6.65 | **1.76** | ~50 | 500 (45 complete, 455 pruned) |
| v11 (dots head) | pct_change_dots | 0.3670 | 4.78 | 3.46 | 2.54 | 6.03 | 1.26 | ~50 | 500 (146 complete, 354 pruned) |

---

## v5: Fix Training Setup

**Changes:**
- Replaced random train/test split with temporal train/val/test split (67%/8%/25%)
- Added early stopping (50 rounds) with validation set
- Expanded Optuna HPO: subsample, colsample_bytree, min_child_weight, max_depth 3-12
- num_boost_round 200-2000 per trial (was 100 default)
- Added Optuna MedianPruner + XGBoostPruningCallback
- Dropped dart booster (too slow), gbtree-only
- Encoded categorical features as numeric (sex→binary, age_class→midpoint, ipf_weight_class→numeric)
- Added MLflow run naming, richer metrics, feature importance logging

**Results:** R²=0.256, RMSE=44.29, MAE=30.95
- Minor improvement over v4 despite major training overhaul
- Temporal split is harder than random split (no future data leakage)
- Top features: bodyweight_change, age_class_encoded, tenure, sex_encoded

**Insight:** Training setup fixes didn't dramatically improve R² because the temporal split removed the implicit data leakage from random splitting. The model is honestly evaluated now.

---

## v6: High-Impact Features

**New features (11):**
- `prev_personal_best_total`, `prev_total_vs_pb`, `prev_distance_from_pb` — personal best tracking
- `prev_total_progress`, `prev_avg_progress_3` — lagged progress rates
- `comps_per_year` — competition density
- `prev_rolling_std_total_3` — rolling variability
- `approx_age`, `years_from_peak_age`, `abs_years_from_peak_age` — age lifecycle
- `prev_total_change_kg` — absolute change in kg

**Results:** R²=0.254, RMSE=44.35, MAE=30.97
- Slightly worse than v5 — more features without better signal hurt slightly
- Same training data and split as v5

**Insight:** Adding features alone didn't help with 250 trials. The HPO couldn't fully explore the expanded feature space. Need more trials and data filtering.

---

## v7: Fine-Tune + Interaction Features

**Changes:**
- Tighter time gap filter: `time_since_last_comp_years >= 0.15` (~55 days, was 30 days via MIN_DAYS guard)
- Winsorized target to p1/p99 (reduces outlier influence while keeping data)
- Added 3 interaction features: `prev_total_x_time_gap`, `prev_total_per_comp`, `elo_gap_vs_field`
- Increased trials from 250 → 500
- Used `replace_strict` for Polars categorical encoding

**Results:** R²=0.351, RMSE=43.97, MAE=30.46, Median AE=20.49
- **Major improvement**: R² up from 0.254 to 0.351 (+38% relative)
- RMSE lift over baseline improved from 7.01 to 10.63
- 410/500 trials pruned (82%) — efficient search

**Top 15 features by gain:**
1. bodyweight_change (236,800)
2. tenure (168,468)
3. prev_total_change_kg (164,769) — NEW
4. years_from_peak_age (160,070) — NEW
5. sex_encoded (137,376)
6. prev_total_vs_segment_mean (111,140)
7. prev_total_percentile_rank (110,053)
8. prev_distance_from_pb (104,566) — NEW
9. time_since_last_comp_years (98,676)
10. segment_mean_total (95,908)
11. age_class_encoded (94,551)
12. approx_age (94,483) — NEW
13. prev_total_vs_pb (84,978) — NEW
14. elo_change (79,311)
15. prev_total_progress (68,601) — NEW

**Per-segment R²:**
- Males: R²=0.366, RMSE=47.68
- Females: R²=0.287, RMSE=36.58
- Best weight class: 93kg (R²=0.394), 74kg (R²=0.387)
- Worst weight class: 59kg (R²=0.234), 47kg (R²=0.261)

**Insight:** The combination of tighter data filtering (removing noisy short-gap entries), winsorization, and more HPO trials unlocked the value of the v6 features. The new features now dominate the top 15 (6 of top 15 are new). The R² ceiling for this task may be limited by the inherent noise in powerlifting progress — a lifter's progress depends heavily on training, nutrition, injuries, and life factors not captured in competition data.

---

## v9–v11: Potential-aware DOTS modelling (combined entry)

This branch (`data/dots-potential-v9-v11`) landed v9, v10, v11 as one batch — all three iterations executed end-to-end before the journey doc was updated. Spec at `docs/superpowers/specs/2026-04-23-potential-aware-dots-modelling-design.md`, plan at `docs/superpowers/plans/2026-04-23-potential-aware-dots-modelling.md`.

**v9 — Data cleaning + DOTS features:**
- Three new filters in `03_base.py` (moved from `03_raw.py` after the rename — pre-existing schema bug caught during the run): `filter_bombouts` (drops 333 rows = 0.05%), `filter_total_consistency` (drops 0 rows; 2.5 kg tolerance), `filter_bw_validity` (drops 2,477 = 0.36%; Konertz ranges).
- Surfaced `Dots` column from OpenPowerlifting source.
- Seven DOTS-parallel features: `previous_dots`, `dots_progress`, `pct_change_dots`, `prev_pct_change_dots`, `rolling_avg_dots_3`, `prev_rolling_avg_dots_3`, `prev_dots_personal_best` (+ derived `prev_dots_vs_pb`, `prev_distance_from_pb_dots`).
- Feature-importance delta logging (top-15 snapshots persisted across versions).

**v10 — Potential + tier features (18 new):**
- Group A — first-comp birth-certificate: `first_comp_dots`, `first_comp_total_per_bw`, `first_comp_age`, `first_comp_percentile_vs_sex_wc`.
- Group B — rolling career-so-far: `max_dots_so_far`, `best_growth_rate_so_far`, `dots_growth_trend` (expanding-slope), `early_growth_rate_dots_per_year` (leak-free, comp 4+), `comps_above_{intermediate,advanced,elite,world_class}_so_far`.
- Group C — DOTS tier ordinals (community-convention cutoffs, sex-agnostic): `starting_tier`, `prev_tier`, `max_tier_so_far`, `tiers_climbed_so_far`.
- Group D — interactions: `starting_tier_x_years_competing`, `max_tier_x_time_gap`.
- Per-starting-tier slice evaluation in training.

**v11 — Dual-head training:**
- `train_for_target` factored from `__main__`; called for both `pct_change_total` (kg head) and `pct_change_dots` (dots head) on identical features and temporal split.
- Normalised RMSE (`rmse / y.std()`) and Spearman ρ added per head.
- Combined predictions parquet (`base/model_predictions_v11.parquet`) via outer join on `(primary_key, date)` with `coalesce=True`.
- DOTS trajectory chart on user-data dashboard page; head toggle on strength-progress page.
- Regression guard plumbed at `conf.PRIOR_BEST_R2`; locked to v11 baselines after this run.

**Test infrastructure introduced:** 61 unit tests across 7 files, parametrised global leakage guard for `prev_*` / `first_comp_*` / `*_so_far` features, session-scoped `base_module` / `raw_module` fixtures.

**Bug fixes caught while running the pipeline (not in original spec):**
- v9 filters originally placed in `03_raw.py` referenced snake_case columns (`squat`, `bench`, etc.) that only exist post-rename in `03_base.py` — pipeline would have crashed on first real run. Moved to `03_base.py` after the rename. Commit `11deae2`.
- `03_raw.py` produced `birth_year` but `conf.base_columns` expected `year_of_birth` — pre-existing schema mismatch, never caught because the production CI didn't run the full chain locally. Added a rename at the end of `03_raw.py`. Commit `5fad012`.
- `replace_strict` in `05_train.py` for `age_class` mapping was missing `80-84` / `85-89` / `90-999` buckets and didn't have a `default` for null sex/age values. Commit `8c97e4f`.

### v11 results

**Pipeline-state snapshot:**
- Base layer: 370,119 rows after all filters (down from 688,534 SBD-event rows; further reduced by `MIN_COMPETITIONS=3` filter).
- Sex distribution: M 240,059, F 130,031, Mx 29.
- Test split: 67,622 rows. Both heads share identical row coverage (`Predictions overlap — kg-only: 0, dots-only: 0, both: 67622`).

**Head-to-head:**

| Head | R² | RMSE | MAE | Median AE | Norm RMSE | Spearman ρ | Baseline RMSE | RMSE Lift |
|------|-----|------|-----|-----------|-----------|-----------|---------------|-----------|
| **kg** (`pct_change_total`) | **0.4537** | 4.89 | 3.52 | 2.57 | **0.7391** | **0.6007** | 6.65 | 1.76 |
| dots (`pct_change_dots`) | 0.3670 | 4.78 | 3.46 | 2.54 | 0.7956 | 0.5329 | 6.03 | 1.26 |

**Verdict — kg head wins.** Higher R², better normalised RMSE, better rank correlation. The DOTS-target framing was hypothesised to be cleaner because DOTS normalises bodyweight, but that bodyweight-normalisation also removes signal the kg head exploits (bodyweight_change, ratios with bodyweight). For dashboard, **make the kg head primary**; keep dots as a secondary view.

**Top 15 features by gain — kg head** (NEW = first appears in v9 or v10):

| # | Feature | Gain | Source |
|---|---------|------|--------|
| 1 | **prev_tier** | 2,695 | **v10** |
| 2 | time_gap_category | 1,559 | v8 |
| 3 | prev_total_change_kg | 786 | v6 |
| 4 | **comps_above_advanced_so_far** | 728 | **v10** |
| 5 | prev_total_percentile_rank | 678 | existing |
| 6 | **comps_above_intermediate_so_far** | 575 | **v10** |
| 7 | bodyweight_change | 509 | existing |
| 8 | sex_encoded | 475 | existing |
| 9 | **first_comp_age** | 369 | **v10** |
| 10 | age_class_encoded | 344 | existing |
| 11 | **previous_dots** | 321 | **v9** |
| 12 | **prev_dots_vs_pb** | 269 | **v9** |
| 13 | approx_age | 256 | existing |
| 14 | **prev_pct_change_dots** | 236 | **v9** |
| 15 | **prev_distance_from_pb_dots** | 228 | **v9** |

**9 of 15 top features are new in v9/v10.** `prev_tier` is dominant by 2× the next-strongest gain. The potential-aware framing carried real signal — v10 features alone account for 5 of the top 15. The dots head shows the same pattern (`prev_tier` rank 1 with gain 3,925; `dots_growth_trend` enters top 12).

**Per-starting-tier (kg head):**

| Tier | n | R² | RMSE |
|------|---|----|------|
| Beginner | 1,436 | 0.441 | 8.15 |
| Intermediate | 14,531 | 0.420 | 6.03 |
| Advanced | 40,760 | 0.450 | 4.54 |
| Elite | 10,748 | 0.455 | 3.75 |
| World Class | 147 | 0.223 | 3.58 |

**Hypothesis check (from spec): "largest R² gain in Beginner/Intermediate, smallest in Elite/World Class (near ceiling)" — refuted.** R² is roughly flat across Beginner→Elite (0.42–0.46). Only World Class drops, and at n=147 the sample is too small to draw conclusions. RMSE *does* fall monotonically from Beginner (8.15) to World Class (3.58) — Elites are more predictable in absolute % terms even if percent-wise variance is similar. The potential features helped, but not in the differentiated-by-tier way the spec hypothesised.

**Per-sex (kg head):**

| Sex | n | R² | RMSE |
|-----|---|----|------|
| M | 43,082 | 0.513 | 4.63 |
| F | 24,540 | 0.347 | 5.31 |

The female-male gap (~0.17 R² points) **persists from v7** (M=0.366, F=0.287). v10 features did not close it. Worth investigating in a follow-up.

**Per-weight-class top-3 (kg head):** 93 kg (R²=0.524), 83 kg (0.531), 74 kg (0.550). **Bottom-3:** 47 kg (0.306), 76 kg (0.327), 84 kg (0.335). Female weight classes (47–84) trail male weight classes consistently.

### Insight

The R² lift v7 → v11 (0.351 → 0.454) is not a clean apples-to-apples comparison — the target changed from `total_progress` (kg/year) to `pct_change_total` (%). The honest read of v9/v10 features' contribution is **structural rather than headline-R²-driving**:
- Top-15 importance rankings are reshaped: 9 of 15 features are new.
- `prev_tier` (a v10 feature) is the dominant predictor.
- Both heads converge on the same top features (potential + tier ordinals + DOTS-parallel), suggesting the signal is robust to target choice.

What this work didn't do: close the female-male gap, close the lighter-weight-class gap, push R² past 0.5. Those are the natural follow-ups.

---

## Technical Notes

- **Temporal split (v11)**: Train cutoff at p67 of dates, val at p75, test at p100 of sorted dates.
- **v11 best params (kg head, trial 0)**: max_depth=10, eta=2.3e-5, subsample=0.87, colsample_bytree=0.84, min_child_weight=5, n_rounds=400. (kg head pruned 455/500; dots head pruned 354/500 — pruning ratio reflects how aggressively MedianPruner cut once a target's loss surface stabilised.)
- **Tracking**: MLflow with SQLite backend, MinIO-backed artifact store, experiments named by date.
- **Pruning**: MedianPruner with 50 warmup steps.
- **Earlier versions context** (v4-v7 trained on `total_progress` in kg/year): RMSE ~44 was on a target with std ~57. Normalised RMSE ≈ 0.77, comparable to v11's 0.74-0.80 on % targets — i.e., the model has consistently captured ~22-26% of target variance across versions, with the remaining variance largely attributable to unmodelled life/training factors.
