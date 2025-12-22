# Sylvan dbt Project

## Overview
This repository contains the **production dbt transformation layer** for Sylvan’s subscription, deal, and invoicing analytics.  
It runs on **Snowflake**, ingests data from **HubSpot and Airtable via Fivetran**, and produces analytics-ready tables used across Finance, RevOps, and Customer Success.

This is a **live, deployed production project**. Changes flow through GitHub and are deployed via CI only.

---

## Stack
- **Warehouse:** Snowflake  
- **Transformation:** dbt (v1.11.0-rc1)  
- **Ingestion:** Fivetran (HubSpot + Airtable)  
- **CI/CD:** GitHub Actions  

---

## Core Models

### `fct_deal_subscription`
**Materialization:** `table`  
**Purpose:** Canonical subscription fact table for analytics and reporting.

**Key characteristics:**
- Built from HubSpot staging models (`stg_deal`, `stg_company`, etc.)
- Enriched with invoice data from Airtable
- Join key:  
  `airtable.INVOICES.associated_hubspot_deal_id → hubspot.deal_id`

**Important logic constraints (intentional):**
- `contract_start_date` and `contract_end_date` **do not exist**
- Subscription timing is derived only from:
  - `booking_date`
  - `contract_term_months`
  - `free_trial_duration`
  - `free_trial_placement`
- `next_invoice_to_raise`:
  - Returns `NULL` when all invoices are raised
  - Never returns placeholder strings (e.g. `"Complete"`)

---

## Sources
- **HubSpot:** via Fivetran staging models
- **Airtable:**  
  - Source: `airtable_sylvan_customer_management`  
  - Table: `INVOICES`

---

## Environments
- **Database:** `SYLVAN_INTERNAL_DATABASE`
- **Schema:** `SUBSCRIPTION_MGMT_DBT`
- **Warehouse:** `SYLVAN_INTERNAL`
- **Role:** `DBT_SYLVAN_ROLE`

---

## Running dbt (Production)
All production runs happen via **GitHub Actions**.

1. Go to **GitHub → Actions**
2. Select the dbt workflow
3. Manually trigger the run
4. CI executes:
   - `dbt build`
   - `dbt test`

No manual dbt runs are performed directly against production.

---

## Notes
- Some source tests are expected to fail and are currently accepted.
- This repository has transitioned from tests-only to a **full transformation project**.
- Treat all changes as production-impacting.

---

# CONTRIBUTING

## Ground Rules
This is a production analytics repository. Changes should be **intentional, minimal, and reviewable**.

Do **not**:
- Re-architect existing models without alignment
- Invent new fields or business logic
- Run dbt directly in production outside CI

---

## Making Changes

### 1. Create a Branch
```bash
git checkout -b feature/<short-description>

2. Edit Models
	•	Update models under models/
	•	Follow existing patterns and naming conventions
	•	Preserve existing business logic unless explicitly changing it

3. Test Locally

Before opening a PR: dbt build
Focus on:
	•	Model compilation
	•	Downstream impact
	•	Tests relevant to your changes

⸻

Pull Requests

PRs should include:
	•	Clear description of what changed and why
	•	Callouts for any logic changes
	•	Confirmation that dbt build was run locally

Avoid large, unfocused PRs.

⸻

Deployment

Deployment is handled only through GitHub Actions.

To deploy:
	1.	Merge your PR into the main branch
	2.	Go to GitHub → Actions
	3.	Manually trigger the dbt workflow

This will rebuild and test production tables in Snowflake.

⸻

Rollbacks

Rollback = revert the commit and re-run the GitHub Action.
There is no manual intervention in Snowflake.
