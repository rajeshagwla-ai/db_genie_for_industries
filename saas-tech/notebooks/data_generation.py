# Databricks notebook source
# MAGIC %md
# MAGIC # Launch Command Center — Synthetic Data Generation
# MAGIC
# MAGIC Generates 7 Delta tables for the **Genie "Launch Command Center"** workshop demo.
# MAGIC The dataset simulates a B2B SaaS company's product-launch performance across
# MAGIC adoption, pipeline, support, marketing, and customer sentiment.
# MAGIC
# MAGIC **Tables produced**
# MAGIC 1. `dim_products` — product / feature catalog
# MAGIC 2. `fct_accounts` — customer accounts
# MAGIC 3. `fct_feature_adoption` — daily product usage
# MAGIC 4. `fct_opportunities` — sales pipeline
# MAGIC 5. `fct_support_tickets` — support tickets
# MAGIC 6. `fct_marketing_campaigns` — launch campaigns
# MAGIC 7. `fct_nps_surveys` — NPS sentiment + verbatims
# MAGIC
# MAGIC **Built-in "aha" patterns**
# MAGIC - One standout launch with strong adoption, pipeline, and NPS lift
# MAGIC - One rough launch with elevated bug tickets and detractor-skewed NPS
# MAGIC - SMB segment shows surprisingly low adoption vs. mid-market / enterprise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Swap the catalog / schema below to retarget the workshop.

# COMMAND ----------

# DBTITLE 1,Parameters
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

# Volume of generated rows (kept inside the requested ranges)
N_PRODUCTS         = 10
N_ACCOUNTS         = 250
N_OPPORTUNITIES    = 500
N_SUPPORT_TICKETS  = 1000
N_CAMPAIGNS        = 40
N_NPS_RESPONSES    = 650

SEED = 42

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DateType, TimestampType,
)

random.seed(SEED)
np.random.seed(SEED)

TODAY = date.today()
HORIZON_START = TODAY - timedelta(days=18 * 30)  # ~18 months ago

FQN = lambda t: f"`{CATALOG}`.`{SCHEMA}`.`{t}`"

# spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE `{CATALOG}`.`{SCHEMA}`")

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Horizon: {HORIZON_START} → {TODAY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `dim_products`
# MAGIC
# MAGIC Curated catalog with staggered launches.
# MAGIC `Insight Copilot` is the **standout** launch; `Realtime Sync` is the **rough** launch.

# COMMAND ----------

products = [
    # name,              tier,         segment,       status,      launch_offset_days_ago
    ("Insight Copilot",   "enterprise", "enterprise", "GA",         120),  # standout
    ("Realtime Sync",     "pro",        "mid-market", "GA",          90),  # rough launch
    ("SmartAlerts",       "pro",        "mid-market", "GA",         300),
    ("Workflow Studio",   "enterprise", "enterprise", "GA",         420),
    ("DataBridge",        "pro",        "mid-market", "GA",         200),
    ("Quickstart Onboard","free",       "SMB",        "GA",         480),
    ("Mobile Companion",  "pro",        "SMB",        "GA",         260),
    ("AuditTrail Pro",    "enterprise", "enterprise", "GA",         360),
    ("ChatOps Bridge",    "pro",        "mid-market", "beta",        45),
    ("LegacyConnect",     "pro",        "mid-market", "deprecated", 510),
][:N_PRODUCTS]

dim_products_pdf = pd.DataFrame([
    {
        "product_id":     f"PROD-{i+1:03d}",
        "product_name":   name,
        "launch_date":    TODAY - timedelta(days=offset),
        "product_tier":   tier,
        "target_segment": segment,
        "status":         status,
    }
    for i, (name, tier, segment, status, offset) in enumerate(products)
])

dim_products_schema = StructType([
    StructField("product_id",     StringType(), False),
    StructField("product_name",   StringType(), False),
    StructField("launch_date",    DateType(),   False),
    StructField("product_tier",   StringType(), False),
    StructField("target_segment", StringType(), False),
    StructField("status",         StringType(), False),
])

(spark.createDataFrame(dim_products_pdf, schema=dim_products_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.dim_products"))

display(spark.table(f"{CATALOG}.{SCHEMA}.dim_products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `fct_accounts`
# MAGIC
# MAGIC Realistic fictional company names across tech-adjacent industries.

# COMMAND ----------

PREFIXES = [
    "Nova", "Lumen", "Quanta", "Helix", "Orbit", "Vantage", "Forge", "Pulse",
    "Beacon", "Cinder", "Halcyon", "Verve", "Strata", "Aether", "Cobalt",
    "Drift", "Echo", "Flint", "Glint", "Harbor", "Iris", "Junction", "Kestrel",
    "Lattice", "Meridian", "Northwind", "Onyx", "Polar", "Quill", "Rivet",
    "Slate", "Tundra", "Umbra", "Vector", "Whisper", "Xenon", "Yarrow", "Zephyr",
    "Apex", "Bramble", "Crescent", "Dune", "Ember", "Fjord", "Granite", "Hollow",
    "Indigo", "Juniper", "Kindred", "Loom",
]

SUFFIXES = [
    "Labs", "Works", "Systems", "Logic", "AI", "Cloud", "Health", "Pay",
    "Sec", "Data", "Forge", "Stack", "Bit", "Wave", "Edge", "Grid", "Vault",
    "Pixel", "Loop", "Flow", "Signal", "Compute", "Metrics", "Analytics",
    "Dynamics", "Networks", "Devices", "Robotics", "Automation", "Solutions",
]

INDUSTRIES = [
    ("fintech", 0.16),   ("healthtech", 0.13), ("martech", 0.11),
    ("devtools", 0.13),  ("security", 0.11),   ("data infra", 0.12),
    ("e-commerce", 0.07),("logistics", 0.06),  ("biotech", 0.05),
    ("edtech", 0.06),
]

REGIONS  = [("NA", 0.55), ("EMEA", 0.30), ("APAC", 0.15)]
PLANS    = [("free", 0.20), ("starter", 0.30), ("pro", 0.30), ("enterprise", 0.20)]
CSM_POOL = [
    "Priya Raman",   "Marcus Chen",    "Sofia Alvarez", "Jordan Hayes",
    "Aisha Okafor",  "Daniel Petrov",  "Lena Park",     "Rohan Mehta",
    "Maya Lindgren", "Tom Whitaker",   "Yuki Tanaka",   "Elena Rossi",
]

def weighted(pairs):
    vals, weights = zip(*pairs)
    return random.choices(vals, weights=weights, k=1)[0]

def make_company_names(n):
    seen = set()
    out  = []
    while len(out) < n:
        name = f"{random.choice(PREFIXES)}{random.choice(SUFFIXES)}"
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out

random.seed(SEED)
np.random.seed(SEED)

names = make_company_names(N_ACCOUNTS)

accounts_rows = []
for i, name in enumerate(names):
    plan       = weighted(PLANS)
    industry   = weighted(INDUSTRIES)
    region     = weighted(REGIONS)
    created    = HORIZON_START + timedelta(days=random.randint(0, 18 * 30 - 30))
    renewal    = created + timedelta(days=365)
    if renewal < TODAY:
        renewal = TODAY + timedelta(days=random.randint(15, 330))

    if plan == "free":
        arr = 0.0
    elif plan == "starter":
        arr = float(np.random.normal(8_000, 2_500))
    elif plan == "pro":
        arr = float(np.random.normal(45_000, 15_000))
    else:  # enterprise
        arr = float(np.random.normal(220_000, 80_000))
    arr = max(0.0, round(arr, 2))

    accounts_rows.append({
        "account_id":            f"ACC-{i+1:05d}",
        "account_name":          name,
        "industry":              industry,
        "arr":                   arr,
        "plan_tier":             plan,
        "csm_owner":             random.choice(CSM_POOL) if plan != "free" else None,
        "region":                region,
        "account_created_date":  created,
        "contract_renewal_date": renewal,
    })

accounts_pdf = pd.DataFrame(accounts_rows)

accounts_schema = StructType([
    StructField("account_id",            StringType(), False),
    StructField("account_name",          StringType(), False),
    StructField("industry",              StringType(), False),
    StructField("arr",                   DoubleType(), False),
    StructField("plan_tier",             StringType(), False),
    StructField("csm_owner",             StringType(), True),
    StructField("region",                StringType(), False),
    StructField("account_created_date",  DateType(),   False),
    StructField("contract_renewal_date", DateType(),   False),
])

(spark.createDataFrame(accounts_pdf, schema=accounts_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_accounts"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_accounts").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `fct_feature_adoption`
# MAGIC
# MAGIC Weekly-rolled-up daily snapshots per (account, product) pair, starting at the
# MAGIC product's launch date. Adoption rates differ by segment / tier so SMB lags
# MAGIC and `Insight Copilot` outperforms.

# COMMAND ----------

random.seed(SEED + 1)
np.random.seed(SEED + 1)

products_pdf = dim_products_pdf
accounts     = accounts_pdf.to_dict("records")

# Segment-conditioned base adoption probabilities for each product
def adoption_prob(product_row, account):
    name    = product_row["product_name"]
    tier    = product_row["product_tier"]
    target  = product_row["target_segment"]
    plan    = account["plan_tier"]

    plan_to_seg = {"free": "SMB", "starter": "SMB",
                   "pro": "mid-market", "enterprise": "enterprise"}
    acc_seg = plan_to_seg[plan]

    base = {"SMB": 0.18, "mid-market": 0.40, "enterprise": 0.55}[acc_seg]

    if target == acc_seg:
        base += 0.20
    elif (target == "enterprise" and acc_seg == "SMB"):
        base -= 0.12

    if name == "Insight Copilot":          # standout
        base += 0.18
    if name == "Realtime Sync":            # rough launch -> middling adoption
        base -= 0.05
    if name == "Quickstart Onboard" and acc_seg == "SMB":
        # SMB segment underperforms even on its own product
        base -= 0.10
    if product_row["status"] == "deprecated":
        base -= 0.25
    if product_row["status"] == "beta":
        base -= 0.10

    return float(np.clip(base, 0.0, 0.85))

adoption_rows = []
for _, prod in products_pdf.iterrows():
    launch = prod["launch_date"]
    days_live = (TODAY - launch).days
    if days_live <= 0:
        continue
    # Weekly snapshots from launch -> today
    weeks = list(range(0, days_live + 1, 7))

    for acc in accounts:
        if acc["account_created_date"] > launch:
            adopt_start_floor = acc["account_created_date"]
        else:
            adopt_start_floor = launch
        if adopt_start_floor >= TODAY:
            continue

        if random.random() > adoption_prob(prod, acc):
            continue  # account never adopts this product

        # First-used date: somewhere between launch and ~60 days post-launch
        max_lag = min(60, (TODAY - adopt_start_floor).days)
        first_lag = int(np.random.exponential(scale=14))
        first_lag = min(first_lag, max(1, max_lag - 1))
        first_used = adopt_start_floor + timedelta(days=first_lag)

        # Plan-tier sized "intensity"
        intensity = {"free": 0.4, "starter": 0.7, "pro": 1.0, "enterprise": 1.6}[acc["plan_tier"]]
        if prod["product_name"] == "Insight Copilot":
            intensity *= 1.4

        for w in weeks:
            event_date = launch + timedelta(days=w)
            if event_date < first_used or event_date > TODAY:
                continue
            ramp = min(1.0, (event_date - first_used).days / 45 + 0.2)
            dau = max(0, int(np.random.poisson(2.5 * intensity * ramp)))
            wau = max(dau, int(dau * np.random.uniform(2.5, 4.5)))
            total_events = int(np.random.poisson(35 * intensity * ramp + 5))

            adoption_rows.append({
                "account_id":           acc["account_id"],
                "product_id":           prod["product_id"],
                "first_used_date":      first_used,
                "event_date":           event_date,
                "daily_active_users":   dau,
                "weekly_active_users":  wau,
                "total_events":         total_events,
            })

adoption_pdf = pd.DataFrame(adoption_rows)
print(f"feature_adoption rows: {len(adoption_pdf):,}")

adoption_schema = StructType([
    StructField("account_id",          StringType(),  False),
    StructField("product_id",          StringType(),  False),
    StructField("first_used_date",     DateType(),    False),
    StructField("event_date",          DateType(),    False),
    StructField("daily_active_users",  IntegerType(), False),
    StructField("weekly_active_users", IntegerType(), False),
    StructField("total_events",        IntegerType(), False),
])

(spark.createDataFrame(adoption_pdf, schema=adoption_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .partitionBy("product_id")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_feature_adoption"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_feature_adoption").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `fct_opportunities`
# MAGIC
# MAGIC Pipeline tied to specific launches; deal sizes correlate with plan tier.
# MAGIC `Insight Copilot` shows a noticeable closed-won surge.

# COMMAND ----------

random.seed(SEED + 2)
np.random.seed(SEED + 2)

STAGES = [
    ("prospecting",   0.18),
    ("qualification", 0.16),
    ("proposal",      0.14),
    ("negotiation",   0.12),
    ("closed-won",    0.25),
    ("closed-lost",   0.15),
]
SOURCES = [("inbound", 0.45), ("outbound", 0.35), ("PLG", 0.20)]

# Per-product weight: standout product gets more opps + better win-rate
prod_weights = {}
for _, p in products_pdf.iterrows():
    w = 1.0
    if p["product_name"] == "Insight Copilot":
        w = 2.4
    elif p["status"] == "deprecated":
        w = 0.3
    elif p["status"] == "beta":
        w = 0.6
    prod_weights[p["product_id"]] = w

prod_ids = list(prod_weights.keys())
prod_w   = np.array(list(prod_weights.values()))
prod_w   = prod_w / prod_w.sum()

opp_rows = []
for i in range(N_OPPORTUNITIES):
    acc = random.choice(accounts)
    prod_id = np.random.choice(prod_ids, p=prod_w)
    prod = products_pdf[products_pdf["product_id"] == prod_id].iloc[0]

    # Created date: between max(account_created, launch_date - 30) and today
    earliest = max(acc["account_created_date"], prod["launch_date"] - timedelta(days=30))
    if earliest >= TODAY:
        continue
    created = earliest + timedelta(days=random.randint(0, (TODAY - earliest).days))

    plan = acc["plan_tier"]
    base_amount = {"free": 5_000, "starter": 18_000, "pro": 60_000, "enterprise": 220_000}[plan]
    amount = max(1_000.0, float(np.random.normal(base_amount, base_amount * 0.35)))

    # Stage selection — boost win-rate for standout product, suppress for rough launch
    stage_weights = dict(STAGES)
    if prod["product_name"] == "Insight Copilot":
        stage_weights["closed-won"] += 0.10
        stage_weights["closed-lost"] -= 0.05
    if prod["product_name"] == "Realtime Sync":
        stage_weights["closed-lost"] += 0.08
        stage_weights["closed-won"]  -= 0.05
    stage_pairs = list(stage_weights.items())
    stage = weighted(stage_pairs)

    if stage in ("closed-won", "closed-lost"):
        close_offset = random.randint(10, 90)
        close_date = created + timedelta(days=close_offset)
        if close_date > TODAY:
            close_date = TODAY - timedelta(days=random.randint(1, 7))
    else:
        close_date = TODAY + timedelta(days=random.randint(15, 180))

    opp_rows.append({
        "opportunity_id": f"OPP-{i+1:06d}",
        "account_id":     acc["account_id"],
        "product_id":     prod_id,
        "stage":          stage,
        "amount":         round(amount, 2),
        "close_date":     close_date,
        "created_date":   created,
        "source":         weighted(SOURCES),
    })

opps_pdf = pd.DataFrame(opp_rows)

opps_schema = StructType([
    StructField("opportunity_id", StringType(), False),
    StructField("account_id",     StringType(), False),
    StructField("product_id",     StringType(), False),
    StructField("stage",          StringType(), False),
    StructField("amount",         DoubleType(), False),
    StructField("close_date",     DateType(),   False),
    StructField("created_date",   DateType(),   False),
    StructField("source",         StringType(), False),
])

(spark.createDataFrame(opps_pdf, schema=opps_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_opportunities"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_opportunities").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `fct_support_tickets`
# MAGIC
# MAGIC Tickets cluster in the first 2-3 weeks post-launch, then taper.
# MAGIC `Realtime Sync` has a markedly higher bug share — the rough-launch signal.

# COMMAND ----------

random.seed(SEED + 3)
np.random.seed(SEED + 3)

CATEGORIES = ["bug", "how-to", "feature-request", "onboarding"]
PRIORITIES = [("low", 0.40), ("medium", 0.35), ("high", 0.18), ("critical", 0.07)]

# Build per-product post-launch ticket weights
def ticket_weight_for_day(days_since_launch):
    # Spike weeks 0-3, then taper
    if days_since_launch < 0:
        return 0.0
    if days_since_launch < 7:
        return 4.0
    if days_since_launch < 14:
        return 3.2
    if days_since_launch < 21:
        return 2.2
    if days_since_launch < 60:
        return 1.2
    return 0.6

# Allocate tickets across products by weighted post-launch volume
prod_ticket_w = {}
for _, p in products_pdf.iterrows():
    days_live = max(1, (TODAY - p["launch_date"]).days)
    base = sum(ticket_weight_for_day(d) for d in range(days_live))
    if p["product_name"] == "Realtime Sync":
        base *= 1.6  # rough launch
    if p["status"] == "deprecated":
        base *= 0.4
    prod_ticket_w[p["product_id"]] = base

total_w = sum(prod_ticket_w.values())
prod_ticket_alloc = {pid: int(round(N_SUPPORT_TICKETS * w / total_w))
                     for pid, w in prod_ticket_w.items()}
# Adjust to exact total
diff = N_SUPPORT_TICKETS - sum(prod_ticket_alloc.values())
if diff != 0:
    first_pid = next(iter(prod_ticket_alloc))
    prod_ticket_alloc[first_pid] += diff

ticket_rows = []
ticket_idx = 0
for _, prod in products_pdf.iterrows():
    n = prod_ticket_alloc[prod["product_id"]]
    if n <= 0:
        continue
    launch = prod["launch_date"]
    days_live = max(1, (TODAY - launch).days)
    day_w = np.array([ticket_weight_for_day(d) for d in range(days_live)])
    day_w = day_w / day_w.sum()

    for _ in range(n):
        ticket_idx += 1
        offset = int(np.random.choice(days_live, p=day_w))
        created = launch + timedelta(days=offset)
        if created > TODAY:
            created = TODAY

        # Category mix
        if prod["product_name"] == "Realtime Sync":
            cat_weights = [("bug", 0.55), ("how-to", 0.20),
                           ("feature-request", 0.10), ("onboarding", 0.15)]
        elif prod["product_name"] == "Quickstart Onboard":
            cat_weights = [("bug", 0.18), ("how-to", 0.30),
                           ("feature-request", 0.07), ("onboarding", 0.45)]
        else:
            cat_weights = [("bug", 0.25), ("how-to", 0.35),
                           ("feature-request", 0.20), ("onboarding", 0.20)]
        category = weighted(cat_weights)

        priority = weighted(PRIORITIES)
        if category == "bug" and random.random() < 0.25:
            priority = random.choice(["high", "critical"])

        # Resolution
        if random.random() < 0.92:
            res_lag = max(1, int(np.random.exponential(scale={
                "low": 6, "medium": 3, "high": 1.5, "critical": 0.7
            }[priority])))
            resolved = created + timedelta(days=res_lag)
            if resolved > TODAY:
                resolved = None
        else:
            resolved = None

        # CSAT — drag down for rough launch, lift for standout
        if resolved is None:
            csat = None
        else:
            mean = 4.0
            if prod["product_name"] == "Realtime Sync":
                mean = 2.8
            if prod["product_name"] == "Insight Copilot":
                mean = 4.5
            if priority in ("high", "critical"):
                mean -= 0.4
            csat = int(np.clip(round(np.random.normal(mean, 1.0)), 1, 5))

        acc = random.choice(accounts)

        ticket_rows.append({
            "ticket_id":    f"TKT-{ticket_idx:06d}",
            "account_id":   acc["account_id"],
            "product_id":   prod["product_id"],
            "category":     category,
            "priority":     priority,
            "created_date": created,
            "resolved_date": resolved,
            "csat_score":   csat,
        })

tickets_pdf = pd.DataFrame(ticket_rows)

tickets_schema = StructType([
    StructField("ticket_id",     StringType(),  False),
    StructField("account_id",    StringType(),  False),
    StructField("product_id",    StringType(),  False),
    StructField("category",      StringType(),  False),
    StructField("priority",      StringType(),  False),
    StructField("created_date",  DateType(),    False),
    StructField("resolved_date", DateType(),    True),
    StructField("csat_score",    IntegerType(), True),
])

(spark.createDataFrame(tickets_pdf, schema=tickets_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_support_tickets"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_support_tickets").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `fct_marketing_campaigns`
# MAGIC
# MAGIC Campaigns cluster around launch dates. **Webinar** outperforms for the
# MAGIC `Insight Copilot` launch (clear channel signal).

# COMMAND ----------

random.seed(SEED + 4)
np.random.seed(SEED + 4)

CHANNELS = ["email", "webinar", "paid", "in-app"]
CAMPAIGN_THEMES = [
    "Launch Wave", "Beta Insider", "What's New", "Customer Spotlight",
    "Workshop Series", "Live Demo", "Industry Briefing", "ROI Calculator",
    "Early Access", "Migration Guide", "Power User Tips", "Roadmap Reveal",
]

prod_list = products_pdf.to_dict("records")
campaign_rows = []
for i in range(N_CAMPAIGNS):
    prod    = random.choice(prod_list)
    channel = random.choice(CHANNELS)
    theme   = random.choice(CAMPAIGN_THEMES)
    launch  = prod["launch_date"]

    start_offset = random.randint(-14, 90)
    start = launch + timedelta(days=start_offset)
    if start > TODAY:
        start = TODAY - timedelta(days=random.randint(2, 30))
    duration = random.randint(7, 45)
    end = min(start + timedelta(days=duration), TODAY)

    base_impr = {
        "email":   np.random.normal(35_000, 8_000),
        "webinar": np.random.normal(2_500,  700),
        "paid":    np.random.normal(120_000, 30_000),
        "in-app":  np.random.normal(18_000, 4_000),
    }[channel]
    impressions = max(500, int(base_impr))

    base_ctr = {"email": 0.04, "webinar": 0.55, "paid": 0.012, "in-app": 0.09}[channel]
    base_signup_rate = {"email": 0.06, "webinar": 0.32, "paid": 0.04, "in-app": 0.18}[channel]
    base_cpm = {"email": 4, "webinar": 60, "paid": 22, "in-app": 2}[channel]

    # Insight Copilot + webinar = standout
    if prod["product_name"] == "Insight Copilot" and channel == "webinar":
        base_ctr *= 1.4
        base_signup_rate *= 1.6
        impressions = int(impressions * 1.3)

    if prod["product_name"] == "Realtime Sync":
        base_signup_rate *= 0.7  # underperforming launch hurts conversion

    clicks  = max(0, int(impressions * np.random.normal(base_ctr, base_ctr * 0.15)))
    signups = max(0, int(clicks * np.random.normal(base_signup_rate, base_signup_rate * 0.20)))
    spend   = round(impressions / 1000.0 * np.random.normal(base_cpm, base_cpm * 0.20), 2)
    spend   = max(spend, 100.0)

    campaign_rows.append({
        "campaign_id":   f"CMP-{i+1:04d}",
        "campaign_name": f"{prod['product_name']} — {theme}",
        "product_id":    prod["product_id"],
        "channel":       channel,
        "impressions":   impressions,
        "clicks":        clicks,
        "signups":       signups,
        "spend":         spend,
        "start_date":    start,
        "end_date":      end,
    })

campaigns_pdf = pd.DataFrame(campaign_rows)

campaigns_schema = StructType([
    StructField("campaign_id",   StringType(),  False),
    StructField("campaign_name", StringType(),  False),
    StructField("product_id",    StringType(),  False),
    StructField("channel",       StringType(),  False),
    StructField("impressions",   IntegerType(), False),
    StructField("clicks",        IntegerType(), False),
    StructField("signups",       IntegerType(), False),
    StructField("spend",         DoubleType(),  False),
    StructField("start_date",    DateType(),    False),
    StructField("end_date",      DateType(),    False),
])

(spark.createDataFrame(campaigns_pdf, schema=campaigns_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_marketing_campaigns"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_marketing_campaigns").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `fct_nps_surveys`
# MAGIC
# MAGIC Score correlated with the account's bug-ticket exposure and adoption breadth.
# MAGIC Verbatims chosen to match the score band.

# COMMAND ----------

random.seed(SEED + 5)
np.random.seed(SEED + 5)

PROMOTER = [
    "Best-in-class — our team finally has a single source of truth.",
    "The new launch is a game changer for how we ship.",
    "Support is responsive and the product just keeps getting better.",
    "Adoption was painless and the ROI showed up within a quarter.",
    "Hard to imagine going back to our old workflow.",
    "Reliable, fast, and the integrations actually work.",
    "Renewing without hesitation — strong product, strong CSM.",
    "The newest features have been spot on for our use case.",
]
PASSIVE = [
    "It works, but I'd like to see better dashboards.",
    "Solid product. Some rough edges on the latest release.",
    "Mostly happy — onboarding could be smoother for new hires.",
    "Useful day-to-day but pricing creeps up at renewal.",
    "Performance is fine; UX could use a refresh.",
    "Reliable for the basics, missing a few power-user features.",
]
DETRACTOR = [
    "We've hit too many bugs since the latest release — losing trust.",
    "Outages this quarter have hurt our team's productivity.",
    "Support escalations take far too long to resolve.",
    "Sync issues caused data loss for our team — not acceptable.",
    "Considering alternatives at renewal if reliability doesn't improve.",
    "The new version broke our existing workflows.",
    "Hard to recommend right now — too many regressions.",
]

# Pre-compute account-level signals
bug_by_acc = (tickets_pdf[tickets_pdf["category"] == "bug"]
              .groupby("account_id").size().to_dict())
adopt_by_acc = (adoption_pdf.groupby("account_id")["product_id"]
                .nunique().to_dict())

acc_lookup = {a["account_id"]: a for a in accounts}

# Bias survey selection toward accounts with activity
acc_ids = list(acc_lookup.keys())
acc_activity = np.array([
    1 + bug_by_acc.get(a, 0) * 0.5 + adopt_by_acc.get(a, 0) * 1.0
    for a in acc_ids
])
acc_p = acc_activity / acc_activity.sum()

nps_rows = []
for i in range(N_NPS_RESPONSES):
    acc_id = np.random.choice(acc_ids, p=acc_p)
    acc = acc_lookup[acc_id]

    bugs    = bug_by_acc.get(acc_id, 0)
    adopted = adopt_by_acc.get(acc_id, 0)

    # Score model: baseline 7, + adoption breadth, - bug exposure
    mean_score = 7.0 + min(adopted, 5) * 0.35 - min(bugs, 8) * 0.55
    if acc["plan_tier"] == "enterprise":
        mean_score += 0.4
    if acc["plan_tier"] == "free":
        mean_score -= 0.3

    score = int(np.clip(round(np.random.normal(mean_score, 1.6)), 0, 10))

    if score >= 9:
        verbatim = random.choice(PROMOTER)
    elif score >= 7:
        verbatim = random.choice(PASSIVE)
    else:
        verbatim = random.choice(DETRACTOR)

    survey_date = HORIZON_START + timedelta(days=random.randint(0, (TODAY - HORIZON_START).days))

    plan_to_seg = {"free": "SMB", "starter": "SMB",
                   "pro": "mid-market", "enterprise": "enterprise"}
    segment = plan_to_seg[acc["plan_tier"]]

    nps_rows.append({
        "response_id": f"NPS-{i+1:06d}",
        "account_id":  acc_id,
        "score":       score,
        "verbatim":    verbatim,
        "survey_date": survey_date,
        "segment":     segment,
    })

nps_pdf = pd.DataFrame(nps_rows)

nps_schema = StructType([
    StructField("response_id", StringType(),  False),
    StructField("account_id",  StringType(),  False),
    StructField("score",       IntegerType(), False),
    StructField("verbatim",    StringType(),  False),
    StructField("survey_date", DateType(),    False),
    StructField("segment",     StringType(),  False),
])

(spark.createDataFrame(nps_pdf, schema=nps_schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.fct_nps_surveys"))

display(spark.table(f"{CATALOG}.{SCHEMA}.fct_nps_surveys").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC Quick row counts and the headline "aha" patterns.

# COMMAND ----------

for tbl in [
    "dim_products", "fct_accounts", "fct_feature_adoption",
    "fct_opportunities", "fct_support_tickets",
    "fct_marketing_campaigns", "fct_nps_surveys",
]:
    n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
    print(f"{tbl:30s} {n:>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aha-moment 1 — standout vs. rough launch (bug share + avg CSAT)

# COMMAND ----------

spark.sql(f"""
SELECT  p.product_name,
        COUNT(*) AS tickets,
        ROUND(100.0 * SUM(CASE WHEN t.category = 'bug' THEN 1 ELSE 0 END) / COUNT(*), 1)
            AS bug_pct,
        ROUND(AVG(t.csat_score), 2) AS avg_csat
FROM    {FQN('fct_support_tickets')} t
JOIN    {FQN('dim_products')}        p USING (product_id)
GROUP BY p.product_name
ORDER BY bug_pct DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aha-moment 2 — SMB segment lags on adoption breadth

# COMMAND ----------

spark.sql(f"""
WITH seg AS (
  SELECT  account_id,
          CASE plan_tier
               WHEN 'free' THEN 'SMB' WHEN 'starter' THEN 'SMB'
               WHEN 'pro'  THEN 'mid-market'
               ELSE 'enterprise' END AS segment
  FROM    {FQN('fct_accounts')}
)
SELECT  s.segment,
        COUNT(DISTINCT s.account_id)                                AS accounts,
        ROUND(COUNT(DISTINCT a.product_id) * 1.0
              / COUNT(DISTINCT s.account_id), 2)                    AS avg_products_per_account
FROM    seg s
LEFT JOIN {FQN('fct_feature_adoption')} a USING (account_id)
GROUP BY s.segment
ORDER BY avg_products_per_account DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aha-moment 3 — webinar wins for the Insight Copilot launch

# COMMAND ----------

spark.sql(f"""
SELECT  p.product_name,
        c.channel,
        SUM(c.signups) AS signups,
        ROUND(SUM(c.spend) / NULLIF(SUM(c.signups), 0), 2) AS cost_per_signup
FROM    {FQN('fct_marketing_campaigns')} c
JOIN    {FQN('dim_products')}            p USING (product_id)
WHERE   p.product_name = 'Insight Copilot'
GROUP BY p.product_name, c.channel
ORDER BY signups DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Sample Genie Questions
# MAGIC %md
# MAGIC ## Sample Genie Questions
# MAGIC
# MAGIC These are the kinds of cross-functional questions a PM, marketer, sales leader, or CS manager might ask the Launch Command Center. Each one requires joining across at least one of the seven tables above.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Adoption & Activation
# MAGIC 1. Which products had the highest week-over-week growth in weekly active users over the last 90 days?
# MAGIC 2. What percentage of enterprise accounts have activated Insight Copilot within 30 days of its launch?
# MAGIC 3. How does the average number of products adopted per account differ across the SMB, mid-market, and enterprise segments?
# MAGIC 4. Which accounts have the highest total event volume this month but were not using the product 60 days ago?
# MAGIC
# MAGIC ### Sales Pipeline
# MAGIC 5. What is the total pipeline value by stage for opportunities tied to products launched in the last 6 months?
# MAGIC 6. Which product launch has generated the most closed-won revenue, and how does its win rate compare to the overall average?
# MAGIC 7. Are inbound or outbound sourced opportunities converting at a higher rate for the Insight Copilot launch?
# MAGIC 8. Which accounts have open opportunities over $100K with a close date in the next 30 days?
# MAGIC
# MAGIC ### Support & Quality
# MAGIC 9. Which product has the highest share of bug-category tickets in the first 3 weeks post-launch?
# MAGIC 10. How does average CSAT score trend week-over-week for Realtime Sync since its launch?
# MAGIC 11. Are critical-priority tickets concentrated in a specific region or industry?
# MAGIC 12. Which CSM owners have the most open high-priority tickets across their book of business right now?
# MAGIC
# MAGIC ### Marketing Effectiveness
# MAGIC 13. What is the cost per signup by channel for each product launch?
# MAGIC 14. Which campaign theme drove the most signups for the Insight Copilot launch, and at what cost?
# MAGIC 15. How do webinar conversion rates compare to paid campaigns across all launches?
# MAGIC
# MAGIC ### Customer Sentiment
# MAGIC 16. What is the current NPS score by product, and which product saw the biggest NPS change in the last quarter?
# MAGIC 17. Which accounts are detractors that also have more than 3 open bug tickets?
# MAGIC 18. Is there a correlation between the number of bug tickets an account has filed and their NPS score?
# MAGIC
# MAGIC ### Executive Summary
# MAGIC 19. Rank each product launch on a composite scorecard: adoption rate, pipeline generated, support ticket volume, and average NPS.
# MAGIC 20. Which launch is underperforming expectations across the most dimensions, and what's the primary signal?
