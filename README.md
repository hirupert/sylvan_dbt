# sylvan_dbt

This repository contains the **dbt project for Sylvan’s Subscription Management analytics**.
It models, tests, and documents data flowing into Snowflake from HubSpot, Airtable, and
related operational systems.

This repo has evolved from a test-only setup into a **production-grade dbt project**
that supports shared ownership, CI enforcement, and extensible business logic.

---

## What This Repository Does

- Declares **Snowflake sources** synced via Fivetran (HubSpot, Airtable)
- Builds **staging models** for cleaned, typed, analytics-ready data
- Builds **mart models** that implement business logic
- Runs **automated dbt tests** locally and in CI
- Generates dbt documentation for visibility and review

---

## Key Model

### `fct_deal_subscription`

Primary fact table used for subscription and invoicing analytics.

This model combines:
- HubSpot deal data
- Contract terms and billing logic
- Airtable invoice records

It computes:
- Subscription start and end dates
- Invoice cadence by billing frequency
- Invoices raised vs. remaining
- Next invoice to raise

Location:

models/marts/subscription_mgmt/fct_deal_subscription.sql

---

## Project Structure

.
├── models/
│   ├── staging/
│   │   ├── hubspot/
│   │   └── airtable/
│   └── marts/
│       └── subscription_mgmt/
├── tests/
├── macros/
├── snapshots/
├── analyses/
├── dbt_project.yml
├── packages.yml
├── README.md
└── CONTRIBUTING.md

---

## Running Locally

### Requirements

- Python 3.10+
- dbt-snowflake
- Snowflake access
- Valid `~/.dbt/profiles.yml`

### Commands

Install dependencies:
```bash
dbt deps

Run models:

dbt run

Run tests:

dbt test

Run everything:

dbt build


⸻

Continuous Integration (CI)

This repository includes a GitHub Actions workflow that:
	1.	Creates a temporary dbt profile
	2.	Installs dependencies
	3.	Runs dbt models and tests
	4.	Fails on breaking issues

Workflow location:

.github/workflows/run_dbt.yml

You can also trigger it manually via:
Actions → Run dbt → Run workflow

⸻

Required GitHub Secrets

Under Settings → Secrets → Actions, configure:
	•	SNOWFLAKE_ACCOUNT
	•	SNOWFLAKE_USER
	•	SNOWFLAKE_ROLE
	•	SNOWFLAKE_WAREHOUSE
	•	SNOWFLAKE_DATABASE
	•	SNOWFLAKE_SCHEMA
	•	SNOWFLAKE_PRIVATE_KEY

These secrets are used exclusively by CI.

⸻

Documentation

Generate and view dbt docs locally:

dbt docs generate
dbt docs serve


⸻

Contribution Rules

All contributors must follow the guidelines in CONTRIBUTING.md.

Direct commits to main are not allowed.

⸻

Ownership & Intent

This repository is a shared production analytics asset.

Treat it like application code:
	•	Small, intentional changes
	•	Clear documentation
	•	Validation before merge

---

### `CONTRIBUTING.md`

```md
# Contributing to sylvan_dbt

This document defines **how teammates should safely edit and extend this dbt project**.

The goal is to enable fast iteration **without breaking production analytics**.

---

## Core Rules

1. Do NOT commit directly to `main`
2. Do NOT edit Snowflake tables manually
3. All transformations must live in dbt
4. Tests protect downstream users — do not remove casually
5. Git history is part of the system of record

---

## Branching Workflow

Always create a feature branch:

```bash
git checkout -b feature/<short-description>

Examples:
	•	feature/add-invoice-logic
	•	feature/update-renewal-calcs
	•	feature/airtable-schema-fix

Open a Pull Request when ready.

⸻

Editing Guidelines

Staging Models (models/staging/)

Purpose
	•	Clean raw source data
	•	Rename columns
	•	Cast types
	•	Filter deleted records

Rules
	•	No aggregations
	•	No metrics
	•	No business logic

If you are unsure whether logic belongs here — it does not.

⸻

Mart Models (models/marts/)

Purpose
	•	Business logic
	•	Metrics
	•	Analytics-ready tables

Rules
	•	Use readable, well-named CTEs
	•	Comment non-obvious logic
	•	Avoid hardcoding assumptions unless explicitly documented
	•	Prefer incremental complexity over monolithic queries

⸻

Sources (sources.yml)

Purpose
	•	Declare upstream tables exactly as they exist in Snowflake

Rules
	•	No transformations
	•	No renaming
	•	Update only when upstream schemas change

If a source changes:
	1.	Update sources.yml
	2.	Update downstream models
	3.	Run dbt build

⸻

Tests

Purpose
	•	Catch regressions early
	•	Protect dashboards and stakeholders

Rules
	•	Do not delete failing tests without explanation
	•	If upstream data breaks a test, document it in the PR
	•	Prefer relaxing tests only when justified

Temporary failures are acceptable only when clearly explained.

⸻

Local Validation

Before opening a PR, run:

dbt deps
dbt run
dbt test

Or scope validation:

dbt run --select <model>
dbt test --select <model>


⸻

Pull Request Requirements

Each PR must include:
	•	What changed
	•	Why it changed
	•	What was validated locally
	•	Known test failures (if any)

Example:

Unified invoice logic to use Airtable INVOICES source.
fct_deal_subscription builds successfully.
Two source tests fail due to upstream null emails — tracked separately.

⸻

CI Expectations

Pull Requests will be blocked if:
	•	Models fail to compile
	•	Critical tests fail
	•	Sources are misdeclared

CI exists to protect shared ownership. Respect it.
