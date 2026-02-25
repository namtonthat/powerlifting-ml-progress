#!/bin/bash
#
# Downloads public parquet files from S3 for local analysis.
# No AWS credentials required.
#

set -e

BUCKET="https://powerlifting-ml-progress.s3.ap-southeast-2.amazonaws.com"
DATA_DIR="data"

FILES=(
  "landing/openpowerlifting-latest.parquet"
  "raw/openpowerlifting-latest.parquet"
  "base/openpowerlifting-latest.parquet"
  "base/describe_by_weight_class.parquet"
  "base/describe_by_experience.parquet"
  "base/describe_by_sex_wc_experience.parquet"
  "base/describe_quality.parquet"
  "base/analysis_career_trajectory.parquet"
  "base/analysis_comp_indexed_growth.parquet"
  "base/analysis_percentile_movement.parquet"
  "base/analysis_rolling_averages.parquet"
  "base/analysis_lift_ratios.parquet"
  "base/analysis_wilks_trajectory.parquet"
  "base/analysis_survival.parquet"
  "base/analysis_archetypes.parquet"
  "base/analysis_archetype_centroids.parquet"
)

echo "==> Downloading data files to ${DATA_DIR}/"

for file in "${FILES[@]}"; do
  dir="${DATA_DIR}/$(dirname "$file")"
  mkdir -p "$dir"
  dest="${DATA_DIR}/${file}"

  if [ -f "$dest" ]; then
    echo "  [skip] ${file} (already exists)"
    continue
  fi

  echo "  [download] ${file}"
  if ! curl -sfL "${BUCKET}/${file}" -o "$dest"; then
    echo "  [warn] ${file} not available, skipping"
    rm -f "$dest"
  fi
done

echo "==> Done. Data files are in ${DATA_DIR}/"
