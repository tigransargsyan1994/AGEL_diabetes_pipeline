# Data Processing Report – AGEL Diabetes Pipeline

## 1. Objective

The goal of this project is to build a small but realistic **healthcare data pipeline** based on the public
**Diabetes 130-US hospitals (1999–2008)** dataset.

The pipeline should:

- Ingest raw hospital encounter data safely (**bronze layer**)
- Check basic data quality (missing values, duplicates, logical rules)
- Transform the data into a clean **silver analytical table**
- Produce a few simple **healthcare metrics** (readmission, LOS, insulin use)

This file documents how the pipeline is designed and what happens to the data.

---

## 2. Architecture – Bronze → Silver

### 2.1 Layers

- **Raw input**
  - `data/raw/diabetic_data.csv`  
    (main encounters table from UCI)
  - `data/raw/ids_mapping.csv`  
    (lookup table for admission type, discharge disposition, admission source)

- **Bronze layer**
  - Ingestion of the raw CSV into a Parquet file.
  - Minimal changes: mostly type `str`, just safe reading.
  - Output:
    - `data/bronze/diabetic_encounters_bronze.parquet`
    - `reports/data_quality/ingestion_report.json`

- **Silver layer**
  - Cleaned, typed, enriched analytical dataset.
  - Encoding of genders, races, and readmission flags.
  - Join of human-readable lookup descriptions.
  - Output:
    - `data/silver/diabetic_encounters_silver.parquet`
    - `data/silver/diabetic_encounters_silver.csv`
    - `reports/silver_export_report.json`
    - summary CSVs in `reports/`

### 2.2 High-Level Flow

```text
data/raw/*.csv
      │
      ▼
[ Ingestion & Logging ]         + [ Parse lookup CSV ]
      │                                   │
      ▼                                   ▼
  Bronze Parquet (encounters + lookup tables)
      │
      ▼
 [ Validation & Data Quality Report ]
      │
      ▼
 [ Transformation to Silver Table ]
      │
      ▼
   Silver Parquet/CSV + Healthcare Summaries
      │
      ▼
 Power BI / Microsoft Fabric Lakehouse
```



---

## 3. Data Quality Findings

Data quality is checked in two steps:

1. **Ingestion summary** – `ingestion_report.json`
2. **Validation report** – `data_quality_report.json`

### 3.1 Ingestion Summary

From `ingestion_report.json`:

* Rows in file (excluding header): ~101 766
* Rows loaded into DataFrame: 101 766
* Rejected rows (estimated): ~0
* Columns loaded: 50

Conclusion: the CSV is structurally consistent and fully ingested.

### 3.2 Missing Values & Duplicates

From `data_quality_report.json`:

* **Missing values**

  * ID and key columns (`encounter_id`, `patient_nbr`, `age`, `gender`) have very few or no missing values.
  * Some fields such as `weight`, `payer_code`, `medical_specialty` have higher missingness – expected for this dataset.

* **Duplicates**

  * Duplicate full rows are rare.
  * `encounter_id` is effectively unique (almost no duplicates by this key).

Conclusion: the dataset is usable for encounter-level analysis with expected gaps in some optional fields.

### 3.3 Logical Checks

The validation also checks simple business rules:

* **Age**

  * Age is stored as brackets like `[60-70)`.
  * Parsed ranges fall between 0 and 120 years.
  * No rows with completely invalid age format.

* **Time in hospital**

  * Converted to numeric and checked against 1–14 days (as per dataset description).
  * Very few (or none) outside this range.

* **Gender**

  * Observed values: `Male`, `Female`, `Unknown/Invalid`.
  * No unexpected gender codes.

These checks help confirm that the data is consistent and reasonable for analysis.

---

## 4. Main Transformations (Silver Table)

The function `transform_encounters(...)` in `pipeline.py` applies the following transformations:

### 4.1 Type Casting

* Numeric columns converted to nullable integers (`Int64`):

  * `time_in_hospital`, `num_lab_procedures`, `num_procedures`,
    `num_medications`, `number_outpatient`, `number_emergency`,
    `number_inpatient`, `number_diagnoses`.
* ID columns cast to string:

  * `encounter_id`, `patient_nbr`, `admission_type_id`,
    `discharge_disposition_id`, `admission_source_id`.

### 4.2 Diagnosis Codes

* Clean `diag_1`, `diag_2`, `diag_3`:

  * Strip whitespace.
  * Upper-case codes.
  * Replace `"?"` and empty strings with `NaN`.

* Store as:

  * `diag_1_clean`, `diag_2_clean`, `diag_3_clean`.

* Group `diag_1_clean` into a **simple diagnosis category**:

  * `diabetes` (ICD-9 `250.xx`)
  * `circulatory` (390–459)
  * `respiratory` (460–519)
  * `digestive` (520–579)
  * `other`

* Stored in:

  * `diag_1_group`

This gives a quick way to analyze encounters by main diagnosis category.

### 4.3 Medication Cleaning & Feature

* Diabetes medication columns: `metformin`, `insulin`, `glyburide`, etc.

* Normalize the status values:

  * `No` → `no`
  * `Steady` → `steady`
  * `Up` → `increased`
  * `Down` → `decreased`
  * `?`, `None` → `NaN`

* New cleaned columns:

  * `<drug>_clean`

* For each cleaned med, create an **active flag**:

  * `steady`, `increased`, `decreased` → `1`
  * `no` → `0`

* Sum of all flags:

  * `num_active_diabetes_meds` – a rough measure of treatment intensity.

### 4.4 Encoded Demographics & Outcomes

* **Gender**

  * `gender_clean`: `"M"`, `"F"`, `"U"`.
  * `gender_female_flag`: `1` for female, `0` for male, `<NA>` for unknown.

* **Race**

  * `race_clean` normalizes race categories to a small set:
    `Caucasian`, `African American`, `Asian`, `Hispanic`, `Other`, `NaN`.

* **Readmission**

  * Source values: `NO`, `<30`, `>30`.
  * `readmitted_any_flag`:

    * 1 if `<30` or `>30`, else 0.
  * `readmitted_30d_flag`:

    * 1 only if `<30`, else 0.

These fields are convenient for reporting and modeling without losing the original raw values.

### 4.5 Lab Results

* Clean and normalize:

  * `A1Cresult` → `a1cresult_clean`
  * `max_glu_serum` → `max_glu_serum_clean`
* Remove placeholder values (`"None"`, `"?"`, empty) and treat them as missing.

### 4.6 Lookup Joins

* Parse `ids_mapping.csv` into three tables:

  * `admission_type_id → admission_type_desc`
  * `discharge_disposition_id → dis_..._desc`
  * `admission_source_id → admission_source_desc`
* Join these descriptions into the main table so that analysts can use friendly text instead of numeric codes.

### 4.7 Column Naming

* All columns are converted to **lowercase snake_case** by replacing spaces, slashes and hyphens with underscores.

This naming is convenient and consistent for Power BI, SQL, and Fabric Lakehouse.

---

## 5. Healthcare Summary Metrics
From the final silver table, the pipeline calculates a few simple metrics:

```markdown
### 5.1 Overall cohort (summary_overall_metrics.csv)

- **Number of encounters:** 101 766  
- **Number of unique patients:** 71 518  
- **Mean length of stay:** ~4.40 days  
- **Median length of stay:** 4 days  
- **Mean number of medications per encounter:** ~16.0  
- **Any readmission rate:** ~46.1%  
- **30-day readmission rate:** ~11.2%

These values come from `reports/summary_overall_metrics.csv`.

---

### 5.2 Readmission by age group (readmission_by_age.csv)

Readmission rates grow with age:

| Age group | Encounters | Readmission rate |
|----------:|-----------:|-----------------:|
| [0-10)    | 161        | 18.0%            |
| [10-20)   | 691        | 38.2%            |
| [20-30)   | 1 657      | 45.0%            |
| [30-40)   | 3 775      | 42.7%            |
| [40-50)   | 9 685      | 44.5%            |
| [50-60)   | 17 256     | 44.0%            |
| [60-70)   | 22 483     | 46.3%            |
| [70-80)   | 26 068     | 48.1%            |
| [80-90)   | 17 197     | 48.3%            |
| [90-100)  | 2 793      | 40.0%            |

---

### 5.3 Readmission by insulin therapy (readmission_by_insulin.csv)

| Insulin status | Encounters | Readmission rate |
|---------------:|-----------:|-----------------:|
| No             | 47 383     | 43.7%            |
| Steady         | 30 849     | 45.1%            |
| Up             | 11 316     | 51.5%            |
| Down           | 12 218     | 52.8%            |

Patients with **changing insulin therapy** (`Up`/`Down`) show higher readmission rates,
which fits the idea that these patients may be more unstable or complex.

---

### 5.4 Race × gender summary (race_gender_summary.csv)

Example rows:

| Race             | Gender | Encounters | Mean LOS (days) | Readmission rate |
|------------------|--------|-----------:|----------------:|-----------------:|
| AfricanAmerican  | Female | 11 728     | 4.54            | 46.1%            |
| AfricanAmerican  | Male   | 7 482      | 4.46            | 45.3%            |
| Caucasian        | Female | 39 689     | 4.48            | 48.0%            |
| Caucasian        | Male   | 36 410     | 4.28            | 45.8%            |
| Asian            | Female | 318        | 3.90            | 35.5%            |
| Asian            | Male   | 323        | 4.09            | 35.0%            |

This table shows how length of stay and readmission vary across demographic groups.
```

---

## 6. Design Decisions & Possible Improvements

### 6.1 Design Decisions

* Use a **bronze/silver** pattern to separate raw ingestion from curated analytics.
* Ingest everything as **strings first** to avoid type issues, then cast in transformations.
* Store bronze/silver in **Parquet** for efficient columnar access.
* Focus on clinically useful features:

  * diagnosis group, readmission flags, medication activity, basic lab status.

### 6.2 Potential Improvements

If this pipeline were extended, some next steps could be:

* **Incremental loads** instead of full reloads (daily or weekly batches).
* More detailed **ICD-9 to clinical group** mapping (e.g. CCS categories).
* Star schema design (fact table + dimension tables).
* Unit tests and CI/CD integration.
* Orchestration using **Airflow** or **Fabric Pipelines**.
* De-identification and governance if working with real patient data.

---

## 7. Optional: Airflow DAG Pseudocode

Below is a simple outline of how this pipeline could look in Airflow:

```python
# PSEUDOCODE ONLY – not full Airflow code

with DAG("agel_diabetes_pipeline", schedule_interval="@daily") as dag:

    ingest_main = PythonOperator(
        task_id="ingest_main_csv",
        python_callable=ingest_main_encounters,
        op_kwargs={"path": str(MAIN_CSV_PATH)},
    )

    ingest_lookups = PythonOperator(
        task_id="ingest_lookup_csv",
        python_callable=parse_lookup_blocks,
        op_kwargs={"path": str(LOOKUP_CSV_PATH)},
    )

    validate = PythonOperator(
        task_id="run_validation",
        python_callable=run_validation,
        op_kwargs={"report_dir": str(DQ_REPORT_DIR)},
    )

    transform = PythonOperator(
        task_id="transform_to_silver",
        python_callable=lambda: transform_and_save(),  # wrapper around transform_encounters + export
    )

    summarize = PythonOperator(
        task_id="generate_summaries",
        python_callable=lambda: generate_summaries_from_disk(),
    )

    [ingest_main, ingest_lookups] >> validate >> transform >> summarize
```

