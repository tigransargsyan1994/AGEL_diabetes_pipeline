import pandas as pd
import numpy as np
import re
import json
import logging
from pathlib import Path
from io import StringIO

# ----------------------------------------------------------------------
# Logging config
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ----------------------------------------------------------------------
# Paths & directories
# ----------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
REPORT_DIR = PROJECT_ROOT / "reports"
DQ_REPORT_DIR = REPORT_DIR / "data_quality"

for d in [RAW_DIR, BRONZE_DIR, SILVER_DIR, REPORT_DIR, DQ_REPORT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# change names if your files are different
MAIN_CSV_PATH = RAW_DIR / "diabetic_data.csv"
LOOKUP_CSV_PATH = RAW_DIR / "ids_mapping.csv"


# ----------------------------------------------------------------------
# Ingestion
# ----------------------------------------------------------------------
def count_data_lines(path, encoding="utf-8"):
    """Count number of *data* lines (excluding header)."""
    with open(path, "r", encoding=encoding) as f:
        return sum(1 for _ in f) - 1


def ingest_main_encounters(path, encoding="utf-8"):
    logging.info(f"Starting ingestion of main CSV from {path}")
    total_lines = count_data_lines(path, encoding=encoding)
    logging.info(f"Total lines in raw file (excluding header): {total_lines}")

    df = pd.read_csv(
        path,
        dtype=str,  # keep everything as string first
        na_values=["?", "NA", "NaN", "null"],
        on_bad_lines="skip",
        encoding=encoding,
    )

    rows_loaded, cols_loaded = df.shape
    rows_rejected = max(total_lines - rows_loaded, 0)

    ingestion_summary = {
        "file": str(path),
        "rows_loaded": int(rows_loaded),
        "columns_loaded": int(cols_loaded),
        "rows_in_file": int(total_lines),
        "rows_rejected_estimated": int(rows_rejected),
        "column_names": df.columns.tolist(),
    }

    logging.info(
        "Ingestion done: %s rows, %s columns, ~%s rejected rows.",
        rows_loaded,
        cols_loaded,
        rows_rejected,
    )

    return df, ingestion_summary


def parse_lookup_blocks(path, encoding="utf-8"):
    """
    Parse the IDs mapping file into three DataFrames:
    - admission_type_df
    - discharge_disposition_df
    - admission_source_df
    """
    with open(path, "r", encoding=encoding) as f:
        text = f.read()

    # Split on lines that are just "," or blank
    blocks = []
    current = []
    for line in text.splitlines():
        if line.strip() in {"", ","}:
            if current:
                blocks.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        blocks.append("\n".join(current))

    if len(blocks) != 3:
        logging.warning("Expected 3 blocks in lookup file, found %s", len(blocks))

    dfs = []
    for block in blocks:
        df_block = pd.read_csv(StringIO(block), dtype=str)
        dfs.append(df_block)

    admission_type_df, discharge_disp_df, admission_source_df = dfs

    # Just in case: use consistent column names
    admission_type_df = admission_type_df.rename(
        columns={"admission_type_id": "admission_type_id"}
    )
    discharge_disp_df = discharge_disp_df.rename(
        columns={"discharge_disposition_id": "discharge_disposition_id"}
    )
    admission_source_df = admission_source_df.rename(
        columns={"admission_source_id": "admission_source_id"}
    )

    return admission_type_df, discharge_disp_df, admission_source_df


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------
def validate_missing_and_duplicates(df):
    n_rows, n_cols = df.shape

    missing = df.isna().sum().reset_index()
    missing.columns = ["column", "missing_count"]
    missing["missing_pct"] = missing["missing_count"] / n_rows

    dup_all_rows = int(df.duplicated().sum())
    if "encounter_id" in df.columns:
        dup_encounters = int(df.duplicated(subset=["encounter_id"]).sum())
    else:
        dup_encounters = None

    return {
        "row_count": int(n_rows),
        "column_count": int(n_cols),
        "missing_by_column": missing.to_dict(orient="records"),
        "duplicates_all_rows": dup_all_rows,
        "duplicates_by_encounter_id": dup_encounters,
    }


def parse_age_range(age_str):
    """Convert age like '[60-70)' into (60, 70), or (None, None) if invalid."""
    if pd.isna(age_str):
        return (None, None)
    age_str = str(age_str).strip()
    if not (age_str.startswith("[") and age_str.endswith(")") and "-" in age_str):
        return (None, None)
    inner = age_str[1:-1]
    parts = inner.split("-")
    if len(parts) != 2:
        return (None, None)
    try:
        low = int(parts[0])
        high = int(parts[1])
        return (low, high)
    except ValueError:
        return (None, None)


def validate_logical_constraints(df):
    issues = {}

    # --- age check ---
    if "age" in df.columns:
        age_ranges = df["age"].dropna().apply(parse_age_range)
        lows = [x[0] for x in age_ranges if x[0] is not None]
        highs = [x[1] for x in age_ranges if x[1] is not None]

        invalid_age_rows = df[
            df["age"].notna()
            & df["age"].apply(lambda x: parse_age_range(x)[0] is None)
        ].shape[0]

        issues["age_min_observed"] = min(lows) if lows else None
        issues["age_max_observed"] = max(highs) if highs else None
        issues["age_invalid_rows"] = int(invalid_age_rows)

        # check against 0–120 rule
        issues["age_out_of_0_120"] = int(
            sum((low < 0 or high > 120) for low, high in zip(lows, highs))
        )

    # --- time_in_hospital check ---
    if "time_in_hospital" in df.columns:
        tih = pd.to_numeric(df["time_in_hospital"], errors="coerce")
        invalid_tih = tih.isna().sum()
        too_low = (tih < 1).sum()
        too_high = (tih > 14).sum()

        issues["time_in_hospital_invalid_rows"] = int(invalid_tih)
        issues["time_in_hospital_less_than_1"] = int(too_low)
        issues["time_in_hospital_greater_than_14"] = int(too_high)

    # --- gender check ---
    if "gender" in df.columns:
        valid_gender = {"Male", "Female", "Unknown/Invalid"}
        unique_gender = set(df["gender"].dropna().unique())
        invalid_gender_values = sorted(list(unique_gender - valid_gender))
        issues["gender_unique_values"] = sorted(list(unique_gender))
        issues["gender_invalid_values"] = invalid_gender_values

    return issues


def run_validation(df, report_dir, report_name="data_quality_report.json"):
    logging.info("Starting data validation...")
    report = {}
    report["missing_and_duplicates"] = validate_missing_and_duplicates(df)
    report["logical_checks"] = validate_logical_constraints(df)

    report_path = report_dir / report_name
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    logging.info("Data quality report written to %s", report_path)
    return report


# ----------------------------------------------------------------------
# Transform helpers
# ----------------------------------------------------------------------
def clean_diag_code(series):
    """Standardize diagnosis codes."""
    s = series.astype("string").str.strip()
    s = s.replace({"?": pd.NA, "": pd.NA})
    s = s.str.upper()
    return s


def standardize_med_status(series):
    """Standardize medication columns."""
    mapping = {
        "No": "no",
        "Steady": "steady",
        "Up": "increased",
        "Down": "decreased",
        "?": pd.NA,
        "NA": pd.NA,
        "None": pd.NA,
    }
    s = series.astype("string").str.strip()
    return s.map(mapping).astype("string")


def encode_gender(df):
    mapping = {
        "Male": "M",
        "Female": "F",
        "Unknown/Invalid": "U",
    }
    df["gender_clean"] = df["gender"].map(mapping).astype("string")

    df["gender_female_flag"] = (
        df["gender_clean"].map({"F": 1, "M": 0, "U": pd.NA})
    ).astype("Int64")
    return df


def encode_race(df):
    race_map = {
        "Caucasian": "Caucasian",
        "AfricanAmerican": "African American",
        "Asian": "Asian",
        "Hispanic": "Hispanic",
        "Other": "Other",
        "?": pd.NA,
        "": pd.NA,
    }
    df["race_clean"] = (
        df["race"].astype("string").str.strip().map(race_map)
    ).astype("string")
    return df


def encode_readmitted(df):
    s = df["readmitted"].astype("string").str.strip().str.upper()
    df["readmitted_raw_clean"] = s

    df["readmitted_any_flag"] = s.replace(
        {"NO": 0, "<30": 1, ">30": 1}
    ).astype("Int64")

    df["readmitted_30d_flag"] = s.replace(
        {"NO": 0, "<30": 1, ">30": 0}
    ).astype("Int64")

    return df


# ----------------------------------------------------------------------
# Transform main
# ----------------------------------------------------------------------
def transform_encounters(df_raw, admission_type_df, discharge_disp_df, admission_source_df):
    df = df_raw.copy()

    # 1) Basic numeric types
    numeric_cols = [
        "time_in_hospital",
        "num_lab_procedures",
        "num_procedures",
        "num_medications",
        "number_outpatient",
        "number_emergency",
        "number_inpatient",
        "number_diagnoses",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # IDs as string
    id_cols = [
        "encounter_id",
        "patient_nbr",
        "admission_type_id",
        "discharge_disposition_id",
        "admission_source_id",
    ]
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # 2) Diagnosis codes
    diag_cols = ["diag_1", "diag_2", "diag_3"]
    for col in diag_cols:
        if col in df.columns:
            df[col + "_clean"] = clean_diag_code(df[col])

    def diag_group(code):
        if code is None or pd.isna(code):
            return None
        code = str(code)
        # diabetes explicitly (ICD-9 250.xx)
        if code.startswith("250"):
            return "diabetes"
        try:
            match = re.match(r"(\d+(\.\d+)?)", code)
            if not match:
                return "other"
            num = float(match.group(1))
        except Exception:
            return "other"
        if 390 <= num <= 459:
            return "circulatory"
        if 460 <= num <= 519:
            return "respiratory"
        if 520 <= num <= 579:
            return "digestive"
        return "other"

    df["diag_1_group"] = df["diag_1_clean"].apply(diag_group).astype("string")

    # 3) Medications
    med_cols = [
        "metformin", "repaglinide", "nateglinide", "chlorpropamide",
        "glimepiride", "acetohexamide", "glipizide", "glyburide",
        "tolbutamide", "pioglitazone", "rosiglitazone", "acarbose",
        "miglitol", "troglitazone", "tolazamide", "examide",
        "citoglipton", "insulin", "glyburide-metformin",
        "glipizide-metformin", "glimepiride-pioglitazone",
        "metformin-rosiglitazone", "metformin-pioglitazone",
    ]

    active_flags = []
    for col in med_cols:
        if col in df.columns:
            clean_col = col + "_clean"
            df[clean_col] = standardize_med_status(df[col])
            flag_col = clean_col + "_active_flag"
            df[flag_col] = df[clean_col].map(
                {"steady": 1, "increased": 1, "decreased": 1, "no": 0}
            ).astype("Int64")
            active_flags.append(flag_col)

    if active_flags:
        df["num_active_diabetes_meds"] = df[active_flags].sum(axis=1).astype("Int64")

    # 4) Encode key attributes
    df = encode_gender(df)
    df = encode_race(df)
    df = encode_readmitted(df)

    # Labs
    lab_cols = ["A1Cresult", "max_glu_serum"]
    for col in lab_cols:
        if col in df.columns:
            s = df[col].astype("string").str.strip()
            s = s.replace({"None": pd.NA, "?": pd.NA, "": pd.NA})
            df[col + "_clean"] = s

    # 5) Join lookup tables
    admission_type_df = admission_type_df.copy()
    discharge_disp_df = discharge_disp_df.copy()
    admission_source_df = admission_source_df.copy()

    admission_type_df["admission_type_id"] = admission_type_df["admission_type_id"].astype("string")
    discharge_disp_df["discharge_disposition_id"] = discharge_disp_df["discharge_disposition_id"].astype("string")
    admission_source_df["admission_source_id"] = admission_source_df["admission_source_id"].astype("string")

    if "description" in admission_type_df.columns:
        admission_type_df = admission_type_df.rename(columns={"description": "admission_type_desc"})
    if "description" in discharge_disp_df.columns:
        discharge_disp_df = discharge_disp_df.rename(columns={"description": "discharge_disposition_desc"})
    if "description" in admission_source_df.columns:
        admission_source_df = admission_source_df.rename(columns={"description": "admission_source_desc"})

    df = df.merge(admission_type_df, on="admission_type_id", how="left")
    df = df.merge(discharge_disp_df, on="discharge_disposition_id", how="left")
    df = df.merge(admission_source_df, on="admission_source_id", how="left")

    # 6) Column naming consistency
    def to_snake(name):
        name = name.replace(" ", "_").replace("-", "_").replace("/", "_")
        return name.lower()

    df.columns = [to_snake(c) for c in df.columns]

    return df


# ----------------------------------------------------------------------
# Summary metrics
# ----------------------------------------------------------------------
def generate_summaries(df_silver, report_dir):
    # Overall summary
    df = df_silver.copy()
    df["time_in_hospital_num"] = pd.to_numeric(df["time_in_hospital"], errors="coerce")
    df["num_medications_num"] = pd.to_numeric(df["num_medications"], errors="coerce")
    readmitted_upper = df["readmitted"].str.upper().str.strip()

    summary_overall = pd.DataFrame([{
        "n_encounters": len(df),
        "n_unique_patients": df["patient_nbr"].nunique(),
        "mean_length_of_stay_days": df["time_in_hospital_num"].mean(),
        "median_length_of_stay_days": df["time_in_hospital_num"].median(),
        "mean_num_medications": df["num_medications_num"].mean(),
        "readmission_rate_any": (readmitted_upper != "NO").mean(),
        "readmission_rate_30d": (readmitted_upper == "<30").mean(),
    }])
    summary_overall.to_csv(report_dir / "summary_overall_metrics.csv", index=False)

    # Readmission by age
    df_age = df_silver.copy()
    df_age["readmitted_any"] = df_age["readmitted"].str.upper().str.strip().ne("NO")
    readmission_by_age = (
        df_age.groupby("age", dropna=False)["readmitted_any"]
        .agg(["count", "mean"])
        .reset_index()
        .rename(columns={"count": "n_encounters", "mean": "readmission_rate"})
        .sort_values("age")
    )
    readmission_by_age.to_csv(report_dir / "readmission_by_age.csv", index=False)

    # Readmission by insulin
    df_ins = df_silver.copy()
    df_ins["readmitted_any"] = df_ins["readmitted"].str.upper().str.strip().ne("NO")
    insulin_readmission = (
        df_ins.groupby("insulin", dropna=False)["readmitted_any"]
        .agg(["count", "mean"])
        .reset_index()
        .rename(columns={"count": "n_encounters", "mean": "readmission_rate"})
        .sort_values("insulin")
    )
    insulin_readmission.to_csv(report_dir / "readmission_by_insulin.csv", index=False)

    # Race & gender
    df_rg = df_silver.copy()
    df_rg["time_in_hospital_num"] = pd.to_numeric(df_rg["time_in_hospital"], errors="coerce")
    df_rg["readmitted_any"] = df_rg["readmitted"].str.upper().str.strip().ne("NO")
    race_gender_summary = (
        df_rg.groupby(["race", "gender"], dropna=False)
        .agg(
            n_encounters=("encounter_id", "count"),
            mean_los_days=("time_in_hospital_num", "mean"),
            readmission_rate=("readmitted_any", "mean"),
        )
        .reset_index()
        .sort_values(["race", "gender"])
    )
    race_gender_summary.to_csv(report_dir / "race_gender_summary.csv", index=False)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    # 1) Ingest main encounters
    df_raw, ingestion_report = ingest_main_encounters(MAIN_CSV_PATH)

    bronze_main_path = BRONZE_DIR / "diabetic_encounters_bronze.parquet"
    df_raw.to_parquet(bronze_main_path, index=False)

    ingestion_report_path = DQ_REPORT_DIR / "ingestion_report.json"
    with open(ingestion_report_path, "w") as f:
        json.dump(ingestion_report, f, indent=2)

    # 2) Data quality report
    run_validation(df_raw, DQ_REPORT_DIR)

    # 3) Parse lookup tables
    admission_type_df, discharge_disp_df, admission_source_df = parse_lookup_blocks(LOOKUP_CSV_PATH)

    # 4) Transform to silver
    df_silver = transform_encounters(df_raw, admission_type_df, discharge_disp_df, admission_source_df)

    silver_parquet_path = SILVER_DIR / "diabetic_encounters_silver.parquet"
    silver_csv_path = SILVER_DIR / "diabetic_encounters_silver.csv"

    df_silver.to_parquet(silver_parquet_path, index=False)
    df_silver.to_csv(silver_csv_path, index=False)

    silver_report = {
        "rows": int(df_silver.shape[0]),
        "columns": int(df_silver.shape[1]),
        "parquet_path": str(silver_parquet_path),
        "csv_path": str(silver_csv_path),
    }
    with open(REPORT_DIR / "silver_export_report.json", "w") as f:
        json.dump(silver_report, f, indent=2)

    # 5) Healthcare summary tables
    generate_summaries(df_silver, REPORT_DIR)

    logging.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
