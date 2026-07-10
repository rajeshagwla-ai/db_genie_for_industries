# Wellness — Lifestyle Risk & Population Health

A Databricks Asset Bundle that deploys a complete Genie space for wellness and
population health analytics, including synthetic data generation.

## What's Included

| File | Purpose |
|------|---------|
| `databricks.yml` | Bundle configuration — variables and targets |
| `resources/health_tech_wellness.job.yml` | Job: generate data, then deploy Genie space |
| `notebooks/generate_data.py` | Creates and populates Unity Catalog tables |
| `src/deploy_genie_space.py` | Creates/updates the Genie space via API |
| `config/serialized_space.json` | Exported Genie space config (instructions, sample questions, certified SQL) |

---

## Prerequisites

Before you start, confirm you have:

- [ ] A Databricks workspace with **Unity Catalog** enabled
- [ ] Permission to create schemas and tables
- [ ] A **SQL Warehouse** running in your workspace
- [ ] **Git** installed on your local machine
- [ ] **Databricks CLI** v0.230+ installed

Check your CLI version:

    databricks --version

Install if needed:
- Mac: `brew install databricks`
- Windows: `winget install Databricks.DatabricksCLI`
- Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`

---

## Setup Instructions

### Step 1: Clone the repository

    git clone https://github.com/rajeshagwla-ai/db_genie_for_industries.git
    cd db_genie_for_industries/health-tech-wellness

### Step 2: Authenticate the Databricks CLI

    databricks configure

When prompted:
- **Databricks Host**: Your workspace URL (e.g., `https://adb-1234567890.12.azuredatabricks.net`)
- **Personal Access Token**: Generate one in your workspace under Profile > Settings > Developer > Access tokens

Verify it works:

    databricks workspace list /

### Step 3: Find your SQL Warehouse ID

In your workspace UI:
1. Click **SQL Warehouses** in the left sidebar
2. Click on your warehouse
3. Click the **Connection Details** tab
4. The **HTTP Path** looks like: `/sql/1.0/warehouses/abc123def456`
5. The warehouse ID is the last segment: `abc123def456`

Or from the CLI:

    databricks warehouses list

### Step 4: Update the data generation notebook

**IMPORTANT:** The notebook `notebooks/generate_data.py` contains hardcoded catalog/schema
references from the original author's workspace. You must update these to match YOUR environment.

Open `notebooks/generate_data.py` in a text editor and find/replace:

| Find | Replace with |
|------|-------------|
| The original catalog name | Your catalog (e.g., `main`) |
| The original schema name  | Your schema name |

Look for lines like:
- `USE CATALOG ...`
- `CREATE SCHEMA ...`
- `catalog = "..."`
- `schema = "..."`

Replace them with your own values. These should match what you put in `variables.yml` in the next step.

### Step 5: Create your personal variable overrides

    mkdir -p .databricks/bundle/dev

**Mac/Linux:**

    cat > .databricks/bundle/dev/variables.yml << 'INNER'
    variables:
      catalog: main
      schema: health_tech
      warehouse_id: PASTE_YOUR_WAREHOUSE_ID_HERE
    INNER

**Windows (PowerShell):**

    New-Item -ItemType Directory -Force -Path .databricks\bundle\dev
    Set-Content -Path .databricks\bundle\dev\variables.yml -Value @"
    variables:
      catalog: main
      schema: health_tech
      warehouse_id: PASTE_YOUR_WAREHOUSE_ID_HERE
    "@

Replace the values:

| Variable | What to put |
|----------|-------------|
| `catalog` | Your Unity Catalog catalog (e.g., `main`) |
| `schema` | Schema name for the tables (will be created if it doesn't exist) |
| `warehouse_id` | Your SQL Warehouse ID from Step 3 |

This file is gitignored — it stays local to you and is never committed.

### Step 6: Validate the bundle

    databricks bundle validate --target dev

You should see:

    Validation OK!

### Step 7: Deploy

    databricks bundle deploy --target dev

This uploads all files and creates the job in your workspace. Nothing runs yet.

### Step 8: Run the job

    databricks bundle run health_tech_wellness --target dev

This executes two tasks in sequence:
1. **generate_data** — Creates tables and populates them with synthetic data
2. **deploy_genie_space** — Creates the Genie space with all instructions and sample questions

A job run URL will be printed — you can click it to monitor progress in the UI.

### Step 9: Open your Genie Space

When the job finishes, the output will print:

    Genie Space URL: https://your-workspace.cloud.databricks.com/genie/rooms/xxxxxxxx

Click that link. You're done! Start asking questions in natural language.

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `generate_data` fails with permission error | Confirm you have CREATE TABLE / CREATE SCHEMA permissions on your catalog |
| `deploy_genie_space` fails | Confirm your warehouse is running and the warehouse_id is correct |
| Validation fails with "cannot resolve variable" | Check that `.databricks/bundle/dev/variables.yml` exists and has all 3 variables |
| Tables created but Genie space shows no data | Make sure the catalog/schema in your `variables.yml` matches what the notebook actually writes to |
| Want to re-run after fixing an issue | Just run `databricks bundle run health_tech_wellness --target dev` again — it updates rather than duplicates |

---

## How It Works (Under the Hood)

1. `databricks bundle deploy` uploads your notebooks, scripts, and config to the workspace
2. The job's first task runs the data generation notebook → creates Unity Catalog tables
3. The job's second task runs `deploy_genie_space.py` which:
   - Reads `config/serialized_space.json` (the exported Genie space config)
   - Rewrites table references to point to YOUR catalog.schema
   - Calls the Databricks API to create (or update) the Genie space
   - Sets permissions so all workspace users can interact with it
   - Prints the URL

---

## Updating the Genie Space

If the maintainer updates the Genie space (new instructions, sample questions, etc.),
pull the latest and re-deploy:

    git pull
    databricks bundle deploy --target dev
    databricks bundle run health_tech_wellness --target dev

Your Genie space will be updated in place — no duplicates created.