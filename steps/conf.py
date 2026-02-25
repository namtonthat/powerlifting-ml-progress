import logging
import re
from enum import Enum

import polars as pl

logging.basicConfig(level=logging.INFO)


class OutputPathType(Enum):
    EXTRACT = "extract"
    LANDING = "landing"
    RAW = "raw"
    BASE = "base"
    SEMANTIC = "semantic"
    REFERENCE = "reference-tables"


class FileLocation(Enum):
    LOCAL = "local"
    S3 = "s3"


# Global Variables
zip_url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
root_data_folder = "data"
bucket_name = "powerlifting-ml-progress"
s3_region = "ap-southeast-2"
parquet_file = "openpowerlifting-latest.parquet"
reference_tables_local_folder_name = "reference-tables"
env_file_path = ".env/project.env"

# OpenPowerlifting original columns (CamelCase, used by Streamlit user data page)
op_cols = ["Name", "Date", "Event", "Wilks", "Best3SquatKg", "Best3BenchKg", "Best3DeadliftKg"]

# This is the name of the minio bucket created
# TODO: Add sync script
artifact_location = "s3://mlflow"

# Reference tables
reference_tables_local_file_path_parquet = f"{root_data_folder}/{reference_tables_local_folder_name}"

# Magic numbers
AGE_TOLERANCE_YEARS = 2  # used to determine if a lifter is the same person
DAYS_IN_YEAR = 365.25
MIN_COMPETITIONS = 3  # minimum comps required per lifter for modelling
MIN_DAYS_BETWEEN_COMPS = 30  # avoid inflated progress rates from back-to-back meets

# Analysis constants
CAREER_TRAJECTORY_TENURE_BUCKETS = [0, 180, 365, 730, 1095, 1825, 2555, 3650]  # days
CAREER_TRAJECTORY_TENURE_LABELS = ["0-6mo", "6-12mo", "1-2yr", "2-3yr", "3-5yr", "5-7yr", "7-10yr"]
SURVIVAL_MILESTONES_M = [300, 400, 500, 600, 700]  # kg total
SURVIVAL_MILESTONES_F = [150, 200, 250, 300, 350]  # kg total
ARCHETYPE_N_CLUSTERS = 5
MAX_CUMULATIVE_COMPS_FOR_INDEXING = 20

# Standard IPF weight classes (upper bound in kg)
IPF_WEIGHT_CLASSES = {
    "M": [59, 66, 74, 83, 93, 105, 120, float("inf")],
    "F": [47, 52, 57, 63, 69, 76, 84, float("inf")],
}


# Functions
def camel_to_snake(camel_str):
    snake_str = re.sub(r"(?<!^)(?=[A-Z])", "_", camel_str).lower()
    return snake_str


# A debug decorator to log the head of the dataframe and the row count
def debug(func):
    def wrapper(*args, **kwargs):
        result_df: pl.DataFrame = func(*args, **kwargs)
        logging.info(result_df.head(2))

        logging.info(f"Row count: {len(result_df)}")
        return result_df

    return wrapper


def create_output_file_path(
    output_path_type: OutputPathType,
    file_location: FileLocation,
    as_http: bool = False,
    file_name: str = parquet_file,
) -> str:
    "Generates the output file path for a given file location and output path type"
    if file_location == FileLocation.LOCAL:
        return f"{root_data_folder}/{output_path_type.value}/{file_name}"
    elif file_location == FileLocation.S3 and not as_http:
        return f"{output_path_type.value}/{file_name}"
    elif file_location == FileLocation.S3 and as_http:
        return f"https://{bucket_name}.s3.{s3_region}.amazonaws.com/{output_path_type.value}/{file_name}"
    else:
        raise ValueError("Invalid file location")


extract_path = f"{root_data_folder}/{OutputPathType.EXTRACT.value}"

# Local
landing_local_file_path = create_output_file_path(
    OutputPathType.LANDING,
    FileLocation.LOCAL,
)
raw_local_file_path = create_output_file_path(
    OutputPathType.RAW,
    FileLocation.LOCAL,
)
base_local_file_path = create_output_file_path(
    OutputPathType.BASE,
    FileLocation.LOCAL,
)

# S3 keys
landing_s3_key = create_output_file_path(
    OutputPathType.LANDING,
    FileLocation.S3,
)
raw_s3_key = create_output_file_path(
    OutputPathType.RAW,
    FileLocation.S3,
)
base_s3_key = create_output_file_path(
    OutputPathType.BASE,
    FileLocation.S3,
)
landing_s3_http = create_output_file_path(OutputPathType.LANDING, FileLocation.S3, as_http=True)

raw_s3_http = create_output_file_path(OutputPathType.RAW, FileLocation.S3, as_http=True)

base_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True)

# Columns
# Landing
_required_landing_column_names = [
    "Date",
    "Name",
    "Sex",
    "Place",
    "Age",
    "AgeClass",
    "BodyweightKg",
    "Event",
    "MeetCountry",
    "Equipment",
    "Best3SquatKg",
    "Best3BenchKg",
    "Best3DeadliftKg",
    "TotalKg",
    "Wilks",
    "Tested",
    "Federation",
    "MeetName",
    "Country",
    "State",
    "ParentFederation",
    "WeightClassKg",
]

renamed_landing_column_names = {
    "Best3SquatKg": "squat",
    "Best3BenchKg": "bench",
    "Best3DeadliftKg": "deadlift",
    "TotalKg": "total",
    "BodyweightKg": "bodyweight",
}

# TODO: Remove this from the raw layer as it is not best practice
# raw columns created from transformations
additional_raw_columns = ["origin_country", "primary_key", "year_of_birth"]

# Base layer
_base_column_names = _required_landing_column_names + additional_raw_columns
base_columns = [camel_to_snake(col) for col in _base_column_names]

base_renamed_columns = {camel_to_snake(key): camel_to_snake(value) for key, value in renamed_landing_column_names.items()}

# Describe layer (summary statistics)
describe_by_weight_class_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="describe_by_weight_class.parquet")
describe_by_experience_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="describe_by_experience.parquet")
describe_by_sex_wc_exp_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="describe_by_sex_wc_experience.parquet")
describe_quality_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="describe_quality.parquet")

describe_by_weight_class_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="describe_by_weight_class.parquet")
describe_by_experience_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="describe_by_experience.parquet")
describe_by_sex_wc_exp_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="describe_by_sex_wc_experience.parquet")
describe_quality_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="describe_quality.parquet")

describe_by_weight_class_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="describe_by_weight_class.parquet")
describe_by_experience_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="describe_by_experience.parquet")
describe_by_sex_wc_exp_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="describe_by_sex_wc_experience.parquet")

# Analysis layer paths
analysis_career_trajectory_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_career_trajectory.parquet")
analysis_career_trajectory_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_career_trajectory.parquet")
analysis_career_trajectory_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_career_trajectory.parquet")

analysis_comp_indexed_growth_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_comp_indexed_growth.parquet")
analysis_comp_indexed_growth_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_comp_indexed_growth.parquet")
analysis_comp_indexed_growth_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_comp_indexed_growth.parquet")

analysis_percentile_movement_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_percentile_movement.parquet")
analysis_percentile_movement_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_percentile_movement.parquet")
analysis_percentile_movement_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_percentile_movement.parquet")

analysis_rolling_averages_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_rolling_averages.parquet")
analysis_rolling_averages_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_rolling_averages.parquet")
analysis_rolling_averages_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_rolling_averages.parquet")

analysis_lift_ratios_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_lift_ratios.parquet")
analysis_lift_ratios_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_lift_ratios.parquet")
analysis_lift_ratios_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_lift_ratios.parquet")

analysis_wilks_trajectory_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_wilks_trajectory.parquet")
analysis_wilks_trajectory_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_wilks_trajectory.parquet")
analysis_wilks_trajectory_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_wilks_trajectory.parquet")

analysis_survival_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_survival.parquet")
analysis_survival_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_survival.parquet")
analysis_survival_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_survival.parquet")

analysis_archetypes_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_archetypes.parquet")
analysis_archetypes_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_archetypes.parquet")
analysis_archetypes_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_archetypes.parquet")

analysis_archetype_centroids_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="analysis_archetype_centroids.parquet")
analysis_archetype_centroids_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="analysis_archetype_centroids.parquet")
analysis_archetype_centroids_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="analysis_archetype_centroids.parquet")
