# K4-Track02-Day17-Data-Pipeline-Engineering

> 🇻🇳 Vietnamese version (default): [`README.md`](README.md)

**Format: individual assignment.** This lab is for K4, Track 02, Day 17.
Assignment repository name: `K4-Track02-Day17-Data-Pipeline-Engineering`.

Build the data pipeline behind an **AI customer-support platform** — the running
example of the Day 17 deck — then **fix three planted bugs** and prove the pipeline
can be re-run, with **checksums**.

```
Postgres tickets ── Debezium CDC ──┐
Kafka support.events ──────────────┼─▶ Bronze ─────────▶ Silver ──────────────┬─▶ gold_doc_chunks    → RAG index
S3 transcripts (JSON) ─────────────┘   Parquet,          MERGE on keys,       ├─▶ gold_training_set  → classifier
                                       immutable         PII masked           └─▶ gold_feature_daily → routing agent
```

Everything runs **zero-key and cross-platform** on DuckDB + Python. No Docker, no
cloud. The dbt track reads the same Bronze.

The diagram describes the source-system scenario. The lab simulates Postgres/CDC,
Kafka and S3 with local JSON/JSONL files in `data/`; those services are not running.
`gold_doc_chunks` uses 16-dimensional hash vectors to exercise chunking and caching,
not semantic embeddings or a complete RAG system. PII regexes mask email addresses
and phone numbers; names and old snapshots are discussed in the reflection questions.

## Learning objectives

- Explain the Bronze, Silver and Gold commitments and parse Debezium CDC.
- Apply keyed writes, preserve the newest state and propagate deletes to Gold.
- Measure lateness from Bronze and handle late arrivals by event time.
- Verify point-in-time training snapshots, embedding caches and safe re-runs.
- Compare the Python pipeline with the dbt implementation.

## Preparation and assignment documents

You need Python **3.10+**, Git, a terminal and a GitHub account for a public submission.
Be familiar with basic Python and SQL joins, aggregation and window functions.
Install `requirements-dbt.txt` for the required dbt section; Docker is only needed for the Airflow bonus.

| Document | Contents |
|---|---|
| [SUBMISSION.md](docs/SUBMISSION.md) | Repository naming, deliverables, deadline and submission checklist |
| [RUBRIC.md](docs/RUBRIC.md) | 100 required points and up to 10 bonus points |
| [CHECKPOINTS.md](docs/CHECKPOINTS.md) | Milestones, expected understanding and self-checks |
| [RULES.md](docs/RULES.md) | AI use, collaboration, late submissions and security |
| [VIBE-CODING.md](docs/VIBE-CODING.md) | Working with an AI coding agent |

The submission, checkpoint and policy guides are in Vietnamese; the rubric is in English.

Assignment guides live in `docs/`, bonus instructions in `docs/bonus/`, pipeline
code in `pipeline/`, and verification and seed utilities in `scripts/`.
Run commands from the repository root. Utilities use `python -m scripts.<name>`,
for example `python -m scripts.verify`. Existing Makefile targets remain the same.

```text
K4-Track02-Day17-Data-Pipeline-Engineering/
├── README.md, README_en.md  # Start here
├── main.py                 # Pipeline entry point
├── pipeline/               # Bronze, Silver, Gold and orchestration
├── scripts/                # Verify, rerun, parity, bonus LLM, seed generator
├── docs/                   # Submission, rubric, checkpoints, rules, AI guide
│   └── bonus/              # Vietnamese and English design challenge
├── tests/                  # Unit tests and contracts
├── data/                   # Seed inputs
├── dbt_project/            # SQL models and dbt configuration
├── docker/                 # Airflow bonus
├── extensions/             # Flywheel and knowledge graph
└── submission/             # Submitted report and checksums
```

---

## Your task (2.5 hours)

This repo **ships with 3 bugs on purpose**. A fresh clone prints `FAILURES` on `make verify`.

1. Run the pipeline, read the failing checks, **find the 3 bugs** in `pipeline/`.
2. **Fix** them — without editing `scripts/verify.py`, `tests/`, `data/` or the checksum logic.
3. Prove it: `make rerun3` re-runs **2026-08-12 three times**; the three Gold checksums
   must be **identical and equal to a fresh build** (`submission/checksums.txt`).
4. dbt track: `make dbt` passes and `make parity` shows both implementations agree.
5. Write `submission/REPORT.md` (analysis ≤ 1 page, excluding command output): for each bug — symptom, root cause, fix,
   deck concept; for each tool choice — why.

Suggested split: 20' read + run · 75' three bugs · 25' dbt · 30' report.

---

## Quick start

For an existing Conda environment (for example `env_vinai_lab`), use its
Python and dbt executables without creating another virtual environment:

```bash
conda activate env_vinai_lab
python -m pip install -r requirements.txt -r requirements-dbt.txt
make VENV="$CONDA_PREFIX" run
make VENV="$CONDA_PREFIX" verify
make VENV="$CONDA_PREFIX" test
make VENV="$CONDA_PREFIX" rerun3
make VENV="$CONDA_PREFIX" lateness
make VENV="$CONDA_PREFIX" dbt
make VENV="$CONDA_PREFIX" parity
```

```bash
make setup        # create .venv + install requirements.txt
make run          # fresh build: reset Silver/Gold, backfill 08-10 .. 08-16 from Bronze
make verify       # 18 contracts — a fresh clone FAILS, that is the lab
make test         # pytest
make rerun3       # THE GRADING TEST: re-run 2026-08-12 three times
make lateness     # measure event lateness from Bronze (P50 / P95 / P99)
```

Without `make` (Windows PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe main.py
.\.venv\Scripts\python.exe -m scripts.verify
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m scripts.rerun_check
.\.venv\Scripts\python.exe main.py --lateness
```

The Makefile assumes a Unix-style shell. Use direct commands on Windows PowerShell;
see [SUBMISSION.md](docs/SUBMISSION.md) for the dbt commands. Failures in the unmodified
starter are expected. The dbt track was tested on Windows with Python 3.11.4,
dbt-core 1.12.5 and dbt-duckdb 1.11.0.

---

## What is in the repo

| File | Layer | What it does | Deck |
|---|---|---|---|
| `data/` | sources | What the source systems deliver each day: Debezium CDC (as Kafka records), Kafka events, transcript exports | Running example |
| `pipeline/bronze.py` | Bronze | Lands each (source, day) as **one immutable Parquet file**; re-landing is a no-op | Bronze commitment |
| `pipeline/staging.py` | Bronze→ | Parses the **Debezium envelope** (`before`/`after`/`op`/`lsn`, tombstones); delete handling needs fixing | Log-based CDC |
| `pipeline/quality.py` | gate | **Pydantic** checks every event; bad → `quarantine_events`, the run never halts | Data testing |
| `pipeline/silver.py` | Silver | Current tickets, **SCD2** history, events, transcripts and PII masking; ticket writes need a keyed upsert | Silver — keyed |
| `pipeline/gold.py` | Gold | `gold_feature_daily` (by **event time**, with **lookback**), `gold_training_set` (**versioned snapshots**, point-in-time), `gold_doc_chunks` (embedding cache keyed by **hash + model version**) | Gold — right shape |
| `pipeline/run.py`, `pipeline/dag.py` | orchestration | One DAG per day; backfill = **the same code path**, day by day | Safe re-runs & backfill |
| `pipeline/checksum.py` | grading | Row-order-independent checksum (plain SQL, runnable in the DuckDB CLI) | The final test |
| `scripts/rerun_check.py` | grading | Fresh build → re-run an old day 3× → compare checksums | Lab 17 |
| `dbt_project/` | dbt | Shared tables `silver_tickets`, `gold_feature_daily`: `merge` + `merge_update_condition`, `microbatch` + `lookback`, contract, unit test | dbt, microbatch |
| `docker/` | bonus | The same daily run on real **Airflow 3** (`airflow.sdk`, `airflow backfill create`) | Airflow 2 → 3 |
| `pipeline/llm_label.py` | bonus | An **LLM labelling** step — naive; you add the hash cache | LLM as a transform |
| `extensions/` | extra | Trace → eval/DPO flywheel and knowledge graph, ungraded | — |

---

## Seed data: the planted stories

Seven days, 2026-08-10 → 2026-08-16, small enough to read by eye
(`scripts/generate_seed.py` regenerates all of it):

Seed dates are simulated data dates, **not the class date or submission deadline**.

- **T-91** is created `low/open` on 08-10 → `high` on 08-14 → `closed/bug` on 08-16
  (the deck's Silver example). The 08-14 change is **delivered twice** by Kafka.
- **T-97** contains a name, an email and a phone number; closed on 08-12; **deleted**
  on 08-15 (erasure request). Debezium sends `op = 'd'` with `after = null`, then a tombstone.
- **u05** is offline on a train on the evening of 08-12: her clicks and a 👎 on T-88
  reach Kafka **on 08-15** — 3 days late.
- On 08-13 a consumer restart redelivers 2 events. 2 events are malformed
  (rating `meh`, missing `user_id`).

---

## The grading test: three checksums

Deck: *"Re-run an old day three times in a row, record the Gold checksum after each
run. The three numbers must be identical."* `make rerun3` does exactly that, one step stricter:

```
fresh build             C0   ← reset Silver/Gold, backfill every day from Bronze
re-run #1 of 2026-08-12 C1
re-run #2 of 2026-08-12 C2
re-run #3 of 2026-08-12 C3   PASS ⇔ C0 = C1 = C2 = C3
```

Why must they equal **C0**, not just each other? A pipeline can be "stably wrong":
the first re-run corrupts the data and every later re-run corrupts it the same way.
Three equal numbers that differ from a fresh build is still a FAIL.

---

## Hints if you are stuck (open one layer at a time)

<details><summary>Silver — <code>silver_tickets</code> has several rows per ticket</summary>

Re-read *"Silver — keyed"* and *"Four ways to write idempotent"*. One row = one
entity needs a **key**. Then: when an **old** batch is re-run after a **newer** one,
which state must win? Which column tells you which change is newer?
</details>

<details><summary>Gold — <code>gold_feature_daily</code> does not reconcile with a full recompute</summary>

Run `make lateness`. Deck *"Late data"*: set the lookback to the P99 of
`(_ingested_at − event_time)` — **measure it from Bronze, don't guess**.
</details>

<details><summary>Deletes — T-97 is still in Silver, the training set and the RAG index</summary>

Open `data/cdc/tickets/2026-08-15.jsonl` and look at the `op = "d"` record. Where is
the ticket's key when `after` is `null`? Deck *"Log-based CDC"* and *"Deletes must propagate"*.
</details>

---

## dbt track (graded)

```bash
make setup-dbt
make dbt          # land Bronze → dbt build: PASS=19 (models + data tests + unit test)
make parity       # silver_tickets + gold_feature_daily: lite vs dbt, same checksum
```

`dbt_project/` implements the two parity tables; it does not implement the Python
pipeline's SCD2 history, transcripts, quarantine, training snapshots or doc chunks.
`silver_tickets` is
`incremental_strategy='merge'` with a `unique_key` and an LSN `merge_update_condition`,
`gold_feature_daily` is `microbatch` (`batch_size='day'`, `lookback=3`), with a
contract, `data_tests:` and a **unit test** for the dedup + delete logic. If
`make parity` reports MISMATCH, one of the two implementations is wrong — usually
the one you have not finished fixing.

---

## Bonus (up to +10, optional)

- **B1 — An LLM step with a cache** (+5): `pipeline/llm_label.py` calls the LLM for
  every ticket on every run and stores whatever comes back. Make `make bonus-llm` print
  `BONUS PASS`: cache key = hash(input) + model + prompt version, a re-run makes 0
  calls, a new prompt re-labels on purpose, off-schema output → quarantine.
  Zero-key: `FakeLLM` stands in for a real model.
- **B2 — pick one** (+5): run the daily pipeline on **Airflow 3** (`make docker-up`,
  follow the [Airflow guide](docs/AIRFLOW.md), screenshot 7 runs and the checksum), **or** the
  real-world brainstorm in [`BONUS-CHALLENGE-EN.md`](docs/bonus/BONUS-CHALLENGE-EN.md).

B1 and B2 together award up to 10 bonus points. The two B2 options do not stack.
These are lab bonus points; skipping the bonus does not reduce the required score.

## Extensions (ungraded)

`make flywheel` (agent traces → eval set + DPO pairs, decontamination, ASOF join) and
`make kg` (knowledge graph vs vector retrieval); see [`extensions/README.md`](extensions/README.md).

---

## Submission

Each learner submits **one public GitHub repository URL** in the K4 / Track 02 / Day 17
LMS assignment, not a PR. Name the submission repository:
`K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`.

The default deadline is **23:59 on the lab day, Asia/Ho_Chi_Minh (UTC+7)** unless
the key coach announces an adjustment. See [SUBMISSION.md](docs/SUBMISSION.md) for the
deliverables and checks, [RULES.md](docs/RULES.md) for policies, and [RUBRIC.md](docs/RUBRIC.md)
for scoring.

New to working with an AI coding agent? Read [`VIBE-CODING.md`](docs/VIBE-CODING.md) first —
and remember: your REPORT must explain every line you changed.

The lakehouse table formats Bronze/Gold land in are **Day 18**; the feature store /
vector DB Gold feeds is **Day 19**; observability and lineage for this pipeline are
**Day 27**.
