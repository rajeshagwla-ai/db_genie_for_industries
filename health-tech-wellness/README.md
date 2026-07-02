# Wellness — Lifestyle Risk & Population Health

A Databricks Asset Bundle that deploys a complete Genie space for wellness and
population health analytics, including synthetic data generation.

## What's Included

| File | Purpose |
|------|---------|
| `databricks.yml` | Bundle configuration — variables and targets |
| `resources/health_tech_wellness.job.yml` | Job with 2 tasks: generate data, then deploy Genie space |
| `notebooks/02_generate_data.py` | Creates and populates Unity Catalog tables with synthetic data |
| `src/deploy_genie_space.py` | Creates/updates the Genie space via API with full config |
| `config/serialized_space.json` | Exported Genie space (instructions, sample questions, SQL) |

## Prerequisites

- Databricks CLI v0.230+ (`databricks --version`)
- A Databricks workspace with Unity Catalog
- A SQL Warehouse

## Setup (one-time, per person)

### 1. Authenticate to your workspace

```bash
databricks configure