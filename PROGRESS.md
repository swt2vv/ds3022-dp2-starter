# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done:

Checkpoint A
    - Virtual environment active, `.env` has `UVA_ID`, and `python kit/smoke_test.py` prints `Emulator OK`
    - Written code for `populate_queue` POSTs to the scatter API and logs queue URL
    - Written code for `get_counts` and `monitor_queue` log the visible, not visible and delayed counts until Delayed reaches 0
    - DAG sketch saved as `docs/dag.jpg`;


- Next:
    Checkpoint A
    - The flow run is **Completed** and visible in the Prefect dashboard; screenshot saved in `docs/`
    - Polling strategy explained in README

    Checkpoint B, C, D

- Blocked:
    - Need to fix bugs in code to have a successflul flow run

## Thu 08 Oct (Lesson 10)
- Done:

    Checkpoint A
    - The flow run is **Completed** and visible in the Prefect dashboard; screenshot saved in `docs/`

    Checkpoint B

    - `collect_messages` receives until 21 fragments are stored or a timeout is reached, logging progress
    - Uses `MessageAttributeNames=["All"]` and `resp.get("Messages", [])`
    - Each fragment is inserted into `raw.fragments` (DuckDB) **before** `delete_message`
    - After a run, all three queue counters are 0 (no dangling messages)
    - Clear failure message if fewer than 21 fragments arrive before the timeout #######W$%^@$#QTAERG


    Checkpoint C
    - `dbt/models/staging/stg_fragments.sql` (integer `order_no`, cleaned `word`, safe against duplicates)
    - `dbt/models/staging/schema.yml` with generic tests
    - A singular test in `dbt/tests/` for "exactly 21 fragments, no gaps"
    - `dbt/models/marts/assembled_phrase.sql` returns the ordered phrase


    - You made the tests **fail on purpose** (delete a row, duplicate a row in `raw.fragments`) and then pass again; screenshots in `docs/` ####Afgsdfgaga
    - The flow's dbt task raises on failure, so submission never runs on a bad phrase ####adfgadfgadfgsd

    Checkpoint D
    - `submit_solution` sends `uvaid`, `phrase`, `platform="prefect"` to `dp2-submit` and logs the HTTP status (200)
    - `python kit/check_submission.py` prints `PASS`; receipt code pasted in your README
    - `git status` shows no `.env`, `.duckdb`, `dbt/target/` or log files


- Next:

    Checkpoint F 
    - Full-delay evidence run (`DP2_FAST=0`) completed; dashboard screenshot and receipt code in README
    - Chaos mode (`DP2_CHAOS=1`): two consecutive runs end in PASS
    - Duplicate messages are counted once
    - Malformed messages are logged or stored **and deleted**; all queue counters are 0 at the end
    - README section "Chaos round": what broke, why, and how you fixed it
    - Repo link and self-assessed rubric submitted by **23:59, Mon 19 Oct**


- Blocked:
    - Unsure how to implement fail on purpose in Checkpoint C


## Mon 19 Oct (Lesson 11)

- Done

    Checkpoint C
    - You made the tests **fail on purpose** (delete a row, duplicate a row in `raw.fragments`) and then pass again; screenshots in `docs/` ####Afgsdfgaga
    - The flow's dbt task raises on failure, so submission never runs on a bad phrase ####adfgadfgadfgsd


    Checkpoint F 
    - Full-delay evidence run (`DP2_FAST=0`) completed; dashboard screenshot and receipt code in README
    - Chaos mode (`DP2_CHAOS=1`): two consecutive runs end in PASS
    - Duplicate messages are counted once
    - Malformed messages are logged or stored **and deleted**; all queue counters are 0 at the end
    - README section "Chaos round": what broke, why, and how you fixed it
    - Repo link and self-assessed rubric submitted by **23:59, Mon 19 Oct**
