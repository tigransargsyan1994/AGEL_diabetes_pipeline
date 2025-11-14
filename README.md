# AGEL Healthcare Data Pipeline – Diabetes 130 US Hospitals

This repo contains a small end-to-end **data pipeline** for hospital encounters of diabetic patients
(using the public *Diabetes 130-US hospitals (1999–2008)* dataset).

The goal is to take **raw CSV** data and produce a **clean analytical dataset** (silver layer) for
reporting in tools like **Power BI** or **Microsoft Fabric**.

---

## 1. Project Structure

```text
AGEL_project/
├── data/
│   ├── raw/       # input CSVs from UCI
│   ├── bronze/    # ingested raw data in Parquet
│   └── silver/    # cleaned analytical table (Parquet + CSV)
├── reports/
│   ├── data_quality/            # JSON reports for ingestion + validation
│   ├── silver_export_report.json
│   ├── summary_overall_metrics.csv
│   ├── readmission_by_age.csv
│   ├── readmission_by_insulin.csv
│   └── race_gender_summary.csv
├── pipeline.py                  # main ETL/ELT script
├── README.md
└── DATA_PROCESSING_REPORT.md
````


---

## 2. Requirements

Tested with:

- Python **3.12**
- [`pandas`](https://pandas.pydata.org/)
- [`numpy`](https://numpy.org/)
- [`pyarrow`](https://arrow.apache.org/docs/python/) – required for Parquet support

Install into your (virtual) environment:

```bash
pip install pandas numpy pyarrow
````

If you use a different environment manager (Conda, Poetry, etc.), install the same packages there.

---

## 3. How to Run the Pipeline

From the project root (`AGEL_project`) run:

```bash
python pipeline.py
```

The script will execute the full ETL/ELT flow:

### 3.1 Ingestion – Bronze Layer

* Reads the main dataset:

  ```text
  data/raw/diabetic_data.csv
  ```

  with safe options:

  * `dtype=str` (avoid type issues at read time)
  * `on_bad_lines="skip"` (skip malformed rows)
  * additional `na_values` for `"?"`, `"NA"`, etc.

* Parses the lookup file:

  ```text
  data/raw/ids_mapping.csv
  ```

  into three small tables mapping:

  * `admission_type_id`
  * `discharge_disposition_id`
  * `admission_source_id`

* Saves the ingested encounters as:

  ```text
  data/bronze/diabetic_encounters_bronze.parquet
  ```

* Writes an ingestion summary report:

  ```text
  reports/data_quality/ingestion_report.json
  ```

---

### 3.2 Data Validation

Runs basic data quality checks on the ingested dataframe:

* **Missing values & duplicates**

  * Missing count and percentage per column.
  * Duplicate rows overall.
  * Duplicate `encounter_id` (if any).

* **Logical rules**

  * Age brackets parse to a range within **0–120** years.
  * `time_in_hospital` in the expected **1–14 days** range.
  * Gender is one of `Male`, `Female`, `Unknown/Invalid`.

Results are written to:

```text
reports/data_quality/data_quality_report.json
```

---

### 3.3 Transformation – Silver Layer

Transforms the raw data into a curated analytical table:

* Casts key numeric columns to nullable integers:

  * `time_in_hospital`, `num_lab_procedures`, `num_procedures`,
    `num_medications`, `number_outpatient`, `number_emergency`,
    `number_inpatient`, `number_diagnoses`.
* Cleans diagnosis codes (`diag_1`, `diag_2`, `diag_3`) into
  `diag_1_clean`, `diag_2_clean`, `diag_3_clean`.
* Groups `diag_1` into broad categories:

  * `diabetes`, `circulatory`, `respiratory`, `digestive`, `other`.
* Standardizes diabetes medication status for many drugs
  (`metformin`, `insulin`, `glyburide`, …):

  * maps `No / Steady / Up / Down / ?` → `no / steady / increased / decreased / NaN`.
  * counts active medications in `num_active_diabetes_meds`.
* Encodes key attributes:

  * `gender_clean` (`M`, `F`, `U`) and `gender_female_flag` (1/0).
  * `race_clean` (normalized race categories).
  * `readmitted_any_flag` (any readmission) and `readmitted_30d_flag` (30-day).
* Cleans lab results:

  * `a1cresult_clean`, `max_glu_serum_clean`.
* Joins readable descriptions from lookup tables:

  * `admission_type_desc`
  * `discharge_disposition_desc`
  * `admission_source_desc`
* Normalizes all column names to **snake_case** and lower case.

Outputs (silver layer):

```text
data/silver/diabetic_encounters_silver.parquet
data/silver/diabetic_encounters_silver.csv
reports/silver_export_report.json
```

---

### 3.4 Healthcare Summary Metrics

As a final step, the script produces four small summary datasets
under `reports/`:

* **Overall cohort metrics**
  `reports/summary_overall_metrics.csv`
  → counts encounters & patients, mean/median length of stay,
  mean number of medications, overall and 30-day readmission rates.

* **Readmission by age group**
  `reports/readmission_by_age.csv`
  → readmission rates by `age` bracket (e.g. `[60-70)`).

* **Readmission by insulin therapy**
  `reports/readmission_by_insulin.csv`
  → readmission rates by `insulin` status (`No`, `Steady`, `Up`, `Down`).

* **Race × gender summary**
  `reports/race_gender_summary.csv`
  → number of encounters, mean length of stay and readmission rate
  per (race, gender) combination.

These CSVs are ready for quick inspection or direct import into Power BI.

---

## 4. Using the Output in Power BI / Microsoft Fabric

### 4.1 Power BI Desktop

1. Open **Power BI Desktop**.

2. Click **Get Data → Parquet**.

3. Browse to:

   ```text
   data/silver/diabetic_encounters_silver.parquet
   ```

4. Build visuals using for example:

* **Demographics**

  * `age`
  * `gender_clean`
  * `race_clean`
* **Outcomes**

  * `time_in_hospital`
  * `readmitted_any_flag`
  * `readmitted_30d_flag`
* **Treatment**

  * `insulin`
  * `num_active_diabetes_meds`
* **Context**

  * `admission_type_desc`
  * `discharge_disposition_desc`
  * `admission_source_desc`

You can also import the summary CSVs (readmission by age, insulin, etc.)
for simpler KPI cards and small tables.

---

### 4.2 Mapping to Microsoft Fabric Components

If this project were deployed on **Microsoft Fabric**, the mapping would look like:

* `data/raw/*.csv`
  → **OneLake / Lakehouse – Bronze area** (raw files)

* `data/bronze/diabetic_encounters_bronze.parquet`
  → **Bronze Lakehouse table** (lightly cleaned but still raw-shaped)

* `data/silver/diabetic_encounters_silver.parquet`
  → **Silver Lakehouse table** (curated analytical view)

* `pipeline.py` logic
  → implemented as a **Fabric Notebook** or **Dataflow Gen2** performing
  the same ingestion, validation and transformation steps.

* **Scheduling / orchestration**
  → a **Fabric Pipeline (Data Factory)** that executes the Notebook/Dataflow
  on a schedule (e.g. daily).

* **Reporting**
  → **Power BI** using Direct Lake or import mode, connected to
  the **silver table** in the Lakehouse.

---

## 5. License & Purpose

* **Dataset**: UCI Machine Learning Repository –
  *Diabetes 130-US hospitals for years 1999–2008*.
* **Usage**: This project is created for an **AGEL BI Engineer / Data Engineer assignment**
  and educational purposes.
  It does **not** use real AGEL patient data and is not intended for production use.

