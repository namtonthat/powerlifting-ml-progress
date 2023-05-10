# Powerlifting ML Progress
**Last Updated:** {{ last_updated }}

This repository analyzes publicly available data from the `OpenPowerlifting` database to estimate how much progress a powerlifter could make over time.

## Usage

To use this repository, you will need to first download the required data from the `OpenPowerlifting` database. You can then run the provided scripts to preprocess the data, train machine learning models, and generate predictions.

## Data

The data used in this repository is sourced from the `OpenPowerlifting` database, which contains information on powerlifting competitions, lifters, and their performances. You can download the necessary data from `s3://{{ bucket_name }}/{{ s3_key }}` file.

## Methodology

The repository uses machine learning models to estimate a lifter's potential progress over time, based on their past competition results. The models are trained on a subset of the `OpenPowerlifting` data and validated using a holdout set.

## Repository Contents

- `data/`: Contains instructions for downloading and preprocessing the `OpenPowerlifting` data.
- `models/`: Contains the machine learning models used for making predictions.
- `scripts/`: Contains the main scripts for preprocessing the data, training the models, and generating predictions.
- `README.md`: This file.

## References

- `OpenPowerlifting` database: https://www.openpowerlifting.org/
