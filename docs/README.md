# DS3022 - Data Project 2 (Fall 2026)

Repo url: https://github.com/swt2vv/ds3022-dp2-starter

This project ...


how to set up and run your pipeline; your polling strategy and why; how your dbt tests protect the result (with a screenshot of a test failing on purpose and then passing); a Prefect dashboard screenshot of one full-delay run (DP2_FAST=0); your receipt code; and a "Chaos round" section (Lesson 11).
- | §4 README, design justification and repository hygiene | ___ / 25 |


## Project structure

- `kit/scatter_api.py`
- `clean.py` — removes duplicates and invalid trips, including zero-passenger, zero-distance, >100 mile, and >1 day records. This is because these are invalid data enries for the purposes of logging true trip events. 
- `transform.py` — adds trip emissions (`trip_co2_kgs`), average speed (`avg_mph`), and date features like hour/day/week/month. It is very useful to have this date data extracted in such a way so we can use the date data to explore the relationship between the month, hour, year, etc. and other variables.  
- `analysis.py` — runs the CO2 summary questions and saves a monthly yellow vs green emissions chart. Outputs a png file which is located in the main folder of the project of the graph generated. 
- `main.py` — runs the full pipeline in order: load -> clean -> transform -> analysis. This makes it very ease to run without determining what to run and what to not run. 
- `dbt/` — contains dbt staging and transformation models for the same workflow. This is another approach to the transformation question not using python, but rather SQL's DBT. 

## How it works
0. Running main.py will do the following:
1. Load raw data into DuckDB
2. Clean the trip tables
3. Add transformed columns for CO2 and time-based calculations
4. Run analysis and save the plot

## Run the project

```bash
pip install -r requirements.txt
python main.py
```

## DBT Approach run

```bash
dbt clean
dbt compile
dbt run
```


## Outputs

- `emissions.duckdb` with cleaned taxi trip tables
- calculated CO2 and time features in the tables
- a monthly emissions comparison chart (`co2_by_month_2024.png`)




# DS3022 - Data Project 2 (Fall 2026)

Repo url: https://github.com/swt2vv/ds3022-dp2-starter

---

Name: Rachel Earl

Student ID: swt2vv

Due Date: 10/20/26


# Setup and Run Instructions

## 1. Clone Repository

```bash
git clone https://github.com/swt2vv/ds3022-dp2-starter
cd <ds3022-dp2-starter>
```

## 2. Create Virtual Environment, Install Dependencies, and Create .env File

```bash
cd ds3022-dp2-starter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Windows:

```bash
cd ds3022-dp2-starter
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

** In your newly created .env file, change the UVA_ID to your specific student ID.


## 3. Set up terminals with necessary programs

### i. Start the SQS emulator (T1)


```bash
moto_server -p 9324
```

What this does:

- ...
- ...



### ii. Start the scatter API (T2)

```bash
python kit/scatter_api.py
```

What this does:

- ...
- ...

### iii. Check everything (T3)

```bash
python kit/smoke_test.py
```

What this does:

- ...
- ...

### iv. Start the Prefect server (T4)

```bash
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
```

What this does:

- ...
- ...


## 4. Run the Pipeline

```bash
python prefect-flow.py
```


Expected output:

```text
...
```

What this does:

- ...
- ...



## 7. Run dbt


```bash
dbt build --project-dir dbt --profiles-dir dbt
```

Expected output:

```text
...
```

What this does:

- ...
- ...


## 8. View Dashboard

Within T4, click "http://127.0.0.1:4200/api" to view Prefect Dashboard. Travel to the Flow page using the navigation bar, select the dp2-pipeline flow, and you can now view each run you execute. To view the unscambled phrase, click on the desired run, and scroll down through the Logs to the Assembled 21 fragments where it is printed. 

![View of Dashboard](docs/View_of_Dashboard.jpg)

---

# Architecture

## DAG Diagram

docs/dag.jpg

## Pipeline Overview

Describe overall flow.

Example:

1. Poll Scatter API
2. Receive fragments
3. Persist raw data
4. Delete processed fragments
5. Run dbt transformations
6. Serve dashboard outputs

---

# Task Boundaries and Dependencies

## Task 1: Poll API

Purpose:

- ...

Inputs:

- ...

Outputs:

- ...

Depends On:

- None

## Task 2: Persist Data

Purpose:

- ...

Inputs:

- ...

Outputs:

- ...

Depends On:

- Poll API

## Task 3: Delete Processed Messages

Purpose:

- ...

Inputs:

- ...

Outputs:

- ...

Depends On:

- Persist Data

## Task 4: dbt Transformations

Purpose:

- ...

Inputs:

- ...

Outputs:

- ...

Depends On:

- Persist Data

---

# Polling Strategy

## Chosen Strategy

Explain:

- How often polling occurs
- Why that interval was chosen
- Tradeoff between responsiveness and API load

## Why This Strategy

Example points:

- Reduces unnecessary API requests
- Keeps latency acceptable
- Simple implementation

## Alternatives Considered

### Option 1: Faster Polling

Rejected because:

- ...

### Option 2: Slower Polling

Rejected because:

- ...

### Option 3: Event-Based/Webhooks

Rejected because:

- ...

---

# Receive → Persist → Delete Order

## Processing Sequence

1. Receive fragment
2. Persist fragment
3. Verify successful persistence
4. Delete from source

## Why This Order Matters

Explain:

- Prevents data loss
- Ensures recovery after failure
- Guarantees messages are not deleted before storage

---

# Idempotency

## What Happens if the Flow Crashes?

### Crash After Receive

Result:

- ...

Recovery:

- ...

### Crash After Persist but Before Delete

Result:

- ...

Recovery:

- ...

### Crash After Delete

Result:

- ...

Recovery:

- ...

## Idempotency Strategy

Explain:

- Unique IDs
- Primary keys
- Deduplication logic
- Upserts / merge logic

---

# Tool Roles

## Prefect

Role:

- Orchestration
- Scheduling
- Retries
- Monitoring

What Would Be Lost Without Prefect?

- ...
- ...

## dbt

Role:

- Data transformations
- Testing
- Documentation

What Would Be Lost Without dbt?

- ...
- ...

## DuckDB

Role:

- Data storage
- Query engine
- Analytics

What Would Be Lost Without DuckDB?

- ...
- ...

---

# Results

## Dashboard Output

docs/dashboard.png

Discussion:

- Key observations
- Business insights

## dbt Output

docs/dbt-output.png

Discussion:

- Models built
- Tests passed

## Receipt Code

```text
PASTE RECEIPT CODE HERE
```

---

# Chaos Round

## What Broke?

Describe the issue encountered.

Example:

- Missing schema
- Empty raw tables
- Failed API calls
- Serialization errors

## Root Cause

Explain why it happened.

## Fix

Explain what you changed.

## Lessons Learned

What would you do differently next time?

---

# Repository Hygiene

## Included

- Source code
- Documentation
- DAG diagram
- Screenshots

## Excluded

The following are intentionally excluded using `.gitignore`:

```text
.env
*.duckdb
dbt/target/
logs/
__pycache__/
```

## Assignment Requirements Verification

- [x] Repository is a fork of assignment repo
- [x] No .env committed
- [x] No DuckDB database committed
- [x] No dbt target directory committed
- [x] No log files committed
- [x] kit/ left unmodified

---

# Future Improvements

- ...
- ...
- ...

---














