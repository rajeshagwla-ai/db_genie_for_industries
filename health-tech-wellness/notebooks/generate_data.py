# Databricks notebook source



import uuid, random, datetime
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

random.seed(42)
np.random.seed(42)

CATALOG = "serverless_91bj8f_ajb_catalog"
START = datetime.date(2023, 1, 1)
END   = datetime.date(2024, 12, 31)

def uid():
    return str(uuid.uuid4())

def rand_date(start=START, end=END):
    return start + datetime.timedelta(days=random.randint(0, (end - start).days))

def write(rows, table):
    try:
        df = spark.createDataFrame(pd.DataFrame(rows))
        df.createOrReplaceTempView("_tmp_write")
        spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.{table} AS SELECT * FROM _tmp_write")
        print(f"  ✓  {table}: {len(rows):,} rows")
    except Exception as e:
        print(f"  ✗  {table}: FAILED — {str(e)[:300]}")
        raise

# ── Name / address data (no faker needed) ──────────────────────────────────
FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
               "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
               "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
               "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
               "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Kenneth","Donna",
               "George","Michelle","Joshua","Carol","Kevin","Amanda","Brian","Melissa",
               "Edward","Deborah"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
               "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
               "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White",
               "Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young",
               "Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores","Green",
               "Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts"]
STREETS     = ["123 Main St","456 Oak Ave","789 Maple Dr","321 Pine Rd","654 Elm St",
               "987 Cedar Ln","147 Birch Blvd","258 Walnut Way","369 Cherry Ct","741 Spruce St"]
CITIES      = ["Los Angeles","Houston","Phoenix","Philadelphia","San Antonio","San Diego",
               "Dallas","Jacksonville","Austin","Fort Worth","Columbus","Charlotte","Indianapolis",
               "San Francisco","Seattle","Denver","Nashville","Louisville","Baltimore","Milwaukee"]
STATES      = ["CA","TX","FL","NY","PA","OH","GA","NC","MI","NJ","VA","WA","AZ","MA",
               "TN","IN","MO","MD","WI","CO"]
EMAIL_DOMS  = ["gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com"]

def rand_name():
    return random.choice(FIRST_NAMES), random.choice(LAST_NAMES)

def rand_email(first, last):
    return f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMS)}"

def rand_phone():
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

def rand_addr():
    return random.choice(STREETS), random.choice(CITIES), random.choice(STATES), \
           f"{random.randint(10000,99999)}"



PAYERS_REF = [
    ("Medicare FFS",         "Medicare",   "Medicare Fee-for-Service",            "800-633-4227", "PO Box 7520, London KY"),
    ("Aetna Medicare Adv",   "Medicare",   "Aetna Medicare Advantage HMO",        "800-633-4227", "PO Box 14079, Lexington KY"),
    ("UHC Medicare Adv",     "Medicare",   "UnitedHealthcare Medicare Advantage",  "800-457-8506", "PO Box 30555, Salt Lake City UT"),
    ("Humana Medicare Adv",  "Medicare",   "Humana Gold Plus HMO",                 "800-833-6917", "PO Box 14601, Lexington KY"),
    ("Medicaid",             "Medicaid",   "State Medicaid Fee-for-Service",       "800-541-5555", "PO Box 6500, Sacramento CA"),
    ("Molina Healthcare",    "Medicaid",   "Molina Managed Medicaid",              "888-562-5442", "PO Box 22668, Long Beach CA"),
    ("WellCare Medicaid",    "Medicaid",   "WellCare Medicaid HMO",                "866-530-9491", "PO Box 31372, Tampa FL"),
    ("Aetna Commercial",     "Commercial", "Aetna Choice POS II",                  "800-872-3862", "PO Box 14079, Lexington KY"),
    ("BCBS Commercial",      "Commercial", "BlueCross BlueShield PPO",             "800-262-2583", "PO Box 1186, Chicago IL"),
    ("Cigna Commercial",     "Commercial", "Cigna Open Access Plus",               "800-244-6224", "PO Box 182223, Chattanooga TN"),
    ("UHC Commercial",       "Commercial", "UnitedHealthcare Choice Plus",         "866-801-4409", "PO Box 740800, Atlanta GA"),
    ("Humana Commercial",    "Commercial", "Humana Preferred PPO",                 "800-448-6262", "PO Box 14601, Lexington KY"),
    ("Anthem BCBS",          "Commercial", "Anthem Blue Cross PPO",                "800-676-2583", "PO Box 105187, Atlanta GA"),
    ("Kaiser Commercial",    "Commercial", "Kaiser Permanente HMO",                "800-464-4000", "PO Box 23219, Oakland CA"),
    ("Oscar Health",         "Commercial", "Oscar Health EPO",                     "855-672-2788", "PO Box 2079, New York NY"),
    ("Bright Health",        "Commercial", "Bright Health HMO",                    "855-274-4441", "PO Box 211580, Eagan MN"),
    ("TriCare",              "Commercial", "TriCare Prime",                        "800-874-2273", "PO Box 7890, Madison WI"),
    ("CHAMPVA",              "Commercial", "CHAMPVA",                              "800-733-8387", "PO Box 65024, Denver CO"),
    ("MultiPlan PPO",        "Commercial", "MultiPlan PPO Network",                "800-950-7040", "PO Box 2999, New York NY"),
    ("Self-Pay",             "Self-Pay",   "Self-Pay / Uninsured",                 None,            None),
]

SPECIALTIES_REF = [
    ("Primary Care",     "Internal Medicine",                    "General Medicine"),
    ("Primary Care",     "Family Medicine",                      "General Medicine"),
    ("Primary Care",     "General Practice",                     "General Medicine"),
    ("Endocrinology",    "Endocrinology, Diabetes & Metabolism", "Endocrinology"),
    ("Endocrinology",    "Endocrinology, Diabetes & Metabolism", "Endocrinology"),
    ("Nephrology",       "Nephrology",                           "Nephrology"),
    ("Ophthalmology",    "Ophthalmology",                        "Ophthalmology"),
    ("Podiatry",         "Podiatric Medicine",                   "Podiatry"),
    ("Cardiology",       "Cardiovascular Disease",               "Cardiology"),
    ("Behavioral Health","Psychiatry",                           "Behavioral Health"),
]

FACILITIES = [
    "B.Well Regional Medical Center", "B.Well Outpatient Clinic North",
    "B.Well Outpatient Clinic South", "B.Well Diabetes Care Center",
    "B.Well Heart & Vascular Institute", "B.Well Behavioral Health Center",
    "B.Well Eye Care", "B.Well Foot & Ankle",
]

DM_ICD10 = [
    ("E11.9",   "Type 2 diabetes mellitus without complications"),
    ("E11.65",  "Type 2 diabetes mellitus with hyperglycemia"),
    ("E11.40",  "Type 2 diabetes mellitus with diabetic neuropathy, unspecified"),
    ("E11.319", "Type 2 diabetes mellitus with unspecified diabetic retinopathy"),
    ("E11.21",  "Type 2 diabetes mellitus with diabetic nephropathy"),
    ("E11.51",  "Type 2 diabetes mellitus with diabetic peripheral angiopathy"),
    ("E11.649", "Type 2 diabetes mellitus with hypoglycemia without coma"),
]

COMORBID_ICD10 = [
    ("I10",    "Essential (primary) hypertension"),
    ("E78.5",  "Hyperlipidemia, unspecified"),
    ("E66.01", "Morbid obesity due to excess calories"),
    ("N18.3",  "Chronic kidney disease, stage 3"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery"),
    ("F32.9",  "Major depressive disorder, single episode, unspecified"),
    ("M17.11", "Primary osteoarthritis, right knee"),
    ("R73.09", "Other abnormal glucose"),
    ("J44.1",  "COPD with acute exacerbation"),
    ("Z87.891","Personal history of nicotine dependence"),
]

# (cpt_code, description, typical_billed_amt)
CPTS = [
    ("83036", "Hemoglobin A1C",                               45.00),
    ("82947", "Glucose, quantitative blood",                   15.00),
    ("80053", "Comprehensive metabolic panel",                 55.00),
    ("80061", "Lipid panel",                                   40.00),
    ("82043", "Urine albumin/creatinine ratio",                25.00),
    ("92014", "Ophthalmological exam, established patient",   180.00),
    ("97597", "Wound debridement, open wound",                120.00),
    ("G0108", "Diabetes self-management training, 30 min",     60.00),
    ("99213", "Office visit, established, low complexity",    150.00),
    ("99214", "Office visit, established, moderate",          250.00),
    ("99215", "Office visit, established, high complexity",   350.00),
    ("93000", "ECG, routine with interpretation",              75.00),
    ("71046", "Chest X-ray, 2 views",                          85.00),
    ("90837", "Psychotherapy, 60 minutes",                    200.00),
    ("99291", "Critical care, first 30-74 minutes",           500.00),
    ("11721", "Debridement of nails, 6 or more",               95.00),
]

# (loinc, name, unit, ref_low, ref_high, mean, std)
LAB_TESTS = [
    ("4548-4",  "HbA1c",                          "%",             4.0,   5.6,   7.8,  1.8),
    ("2345-7",  "Fasting Glucose",                "mg/dL",        70.0,  99.0, 145.0, 55.0),
    ("2160-0",  "Creatinine, Serum",              "mg/dL",         0.6,   1.2,   1.1,  0.5),
    ("33914-3", "eGFR",                           "mL/min/1.73m2",60.0, 120.0,  72.0, 22.0),
    ("14959-1", "Urine Albumin/Creatinine Ratio", "mg/g",          0.0,  30.0,  55.0, 80.0),
    ("2093-3",  "Total Cholesterol",              "mg/dL",         0.0, 200.0, 195.0, 38.0),
    ("13457-7", "LDL Cholesterol",               "mg/dL",          0.0, 100.0, 112.0, 35.0),
    ("2085-9",  "HDL Cholesterol",               "mg/dL",         40.0,  60.0,  48.0, 12.0),
    ("2571-8",  "Triglycerides",                 "mg/dL",          0.0, 150.0, 168.0, 85.0),
    ("718-7",   "Hemoglobin",                    "g/dL",          12.0,  17.5,  13.5,  1.5),
    ("6690-2",  "WBC Count",                     "10*3/uL",        4.0,  11.0,   7.2,  2.0),
    ("2947-0",  "Sodium, Serum",                 "mmol/L",       136.0, 145.0, 139.0,  3.0),
]

# (ndc, brand, generic, drug_class, dose_val, dose_unit, route)
MEDS_REF = [
    ("00093-1074-01", "Metformin 500mg",  "Metformin HCl",     "Biguanides",       500,  "mg",    "Oral"),
    ("00093-1074-05", "Metformin 1000mg", "Metformin HCl",     "Biguanides",       1000, "mg",    "Oral"),
    ("00169-4161-11", "Lantus",           "Insulin Glargine",  "Insulin",          100,  "units", "Subcutaneous"),
    ("00169-4060-12", "Victoza",          "Liraglutide",       "GLP-1 Agonist",    1.2,  "mg",    "Subcutaneous"),
    ("00169-4175-15", "Ozempic",          "Semaglutide",       "GLP-1 Agonist",    0.5,  "mg",    "Subcutaneous"),
    ("00597-0114-37", "Jardiance",        "Empagliflozin",     "SGLT2 Inhibitor",  10,   "mg",    "Oral"),
    ("00006-0221-31", "Januvia",          "Sitagliptin",       "DPP-4 Inhibitor",  100,  "mg",    "Oral"),
    ("00071-0155-23", "Glucotrol XL",     "Glipizide",         "Sulfonylurea",     5,    "mg",    "Oral"),
    ("00093-5460-10", "Lisinopril",       "Lisinopril",        "ACE Inhibitor",    10,   "mg",    "Oral"),
    ("00071-0156-23", "Lipitor",          "Atorvastatin",      "Statin",           40,   "mg",    "Oral"),
    ("00006-0749-31", "Cozaar",           "Losartan",          "ARB",              50,   "mg",    "Oral"),
    ("00310-0271-90", "Farxiga",          "Dapagliflozin",     "SGLT2 Inhibitor",  10,   "mg",    "Oral"),
    ("00024-5900-10", "Trulicity",        "Dulaglutide",       "GLP-1 Agonist",    0.75, "mg",    "Subcutaneous"),
]

DENIAL_DATA = {
    "Authorization":    [("CO-15",  "Payment adjusted — authorization absent",      "N130"),
                         ("CO-197", "Precertification/authorization absent",          "N244"),
                         ("CO-4",   "Service inconsistent — auth required",           "N519")],
    "Coding":           [("CO-11",  "Diagnosis inconsistent with procedure",          "N10"),
                         ("CO-16",  "Claim lacks information for adjudication",       "N242"),
                         ("CO-252", "Attachment/documentation required",              "N706")],
    "Eligibility":      [("CO-27",  "Expenses incurred after coverage terminated",   "N30"),
                         ("CO-31",  "Patient cannot be identified as insured",       "N180"),
                         ("CO-96",  "Non-covered charge(s)",                          "N130")],
    "Duplicate":        [("CO-18",  "Exact duplicate claim/service",                  "N522")],
    "Timely Filing":    [("CO-29",  "Time limit for filing has expired",              "N29")],
    "Medical Necessity":[("CO-50",  "Non-covered — not medically necessary",          "N115"),
                         ("CO-167", "Diagnosis not covered per plan",                 "N130")],
}

EMPLOYER_GROUPS = ["KaiserPermanente","HCA_Healthcare","MayoClinic","Ascension",
                   "CommonSpirit","Dignity_Health","Tenet_Healthcare","AdventHealth",
                   "Northwell_Health","Sutter_Health"]
PHARMACIES = ["CVS Pharmacy","Walgreens","Rite Aid","Walmart Pharmacy",
              "Kroger Pharmacy","Costco Pharmacy","Express Scripts Mail Order"]
HIGH_DENIAL_PAYERS = {"Medicaid","WellCare Medicaid","Molina Healthcare"}
SLOW_AUTH_PAYERS   = {"Medicaid","UHC Medicare Adv","WellCare Medicaid"}

print("Reference data loaded.")



print("Generating payers...")
payer_rows, payer_ids = [], []
for name, ptype, plan, phone, addr in PAYERS_REF:
    pid = uid()
    payer_ids.append(pid)
    payer_rows.append({"payer_id": pid, "payer_name": name, "payer_type": ptype,
                        "plan_name": plan, "group_number": f"GRP{random.randint(10000,99999)}",
                        "phone_number": phone, "claims_address": addr,
                        "state": random.choice(STATES)})
payer_name_map = {r["payer_id"]: r["payer_name"] for r in payer_rows}
write(payer_rows, "core.payers")



print("Generating providers...")
provider_rows, provider_ids = [], []
for i in range(200):
    dept, spec, _ = random.choice(SPECIALTIES_REF)
    pid = uid()
    provider_ids.append(pid)
    fn, ln = rand_name()
    addr1, city, state, zc = rand_addr()
    provider_rows.append({
        "provider_id": pid,
        "npi": str(random.randint(1000000000, 9999999999)),
        "first_name": fn, "last_name": ln,
        "credential": random.choice(["MD","MD","MD","DO","NP","PA"]),
        "specialty": spec, "department": dept,
        "facility_name": random.choice(FACILITIES),
        "address_line1": addr1, "city": city, "state": state, "zip_code": zc,
        "phone_number": rand_phone(),
        "tax_id": f"{random.randint(10,99)}-{random.randint(1000000,9999999)}",
    })
write(provider_rows, "core.providers")



print("Generating patients...")
patient_rows, patient_ids, diabetic_ids = [], [], []
for i in range(1000):
    pid = uid()
    patient_ids.append(pid)
    is_dm = (i < 500)
    if is_dm:
        diabetic_ids.append(pid)
    age = random.randint(35, 82) if is_dm else random.randint(22, 75)
    dob = datetime.date(2025, 1, 1) - datetime.timedelta(days=age*365 + random.randint(0, 364))
    fn, ln = rand_name()
    addr1, city, state, zc = rand_addr()
    patient_rows.append({
        "patient_id": pid, "first_name": fn, "last_name": ln,
        "date_of_birth": dob,
        "gender": random.choice(["M","M","F","F","F"]),
        "race": random.choice(["White","Black or African American","Hispanic","Asian","Other"]),
        "ethnicity": random.choice(["Not Hispanic or Latino","Not Hispanic or Latino","Hispanic or Latino"]),
        "address_line1": addr1, "address_line2": None,
        "city": city, "state": state, "zip_code": zc,
        "phone_number": rand_phone(), "email": rand_email(fn, ln),
        "insurance_member_id": f"MBR{random.randint(100000000,999999999)}",
        "payer_id": random.choice(payer_ids),
        "primary_care_provider_id": random.choice(provider_ids),
        "employer_group_id": random.choice(EMPLOYER_GROUPS),
        "created_at": datetime.datetime(2023, 1, 1),
    })
patient_payer_map = {r["patient_id"]: r["payer_id"] for r in patient_rows}
patient_employer_map = {r["patient_id"]: r["employer_group_id"] for r in patient_rows}
diabetic_set = set(diabetic_ids)
poorly_controlled_set = set(random.sample(diabetic_ids, k=200))  # 200/500 poorly controlled
write(patient_rows, "core.patients")



print("Generating encounters...")
enc_rows, enc_ids = [], []
patient_enc_map = {p: [] for p in patient_ids}
for _ in range(8000):
    eid = uid()
    enc_ids.append(eid)
    pid = random.choice(patient_ids)
    admit = rand_date()
    etype = random.choices(["Outpatient","Telehealth","Inpatient","Emergency"],
                           weights=[55, 20, 15, 10])[0]
    is_dm = pid in diabetic_set
    patient_enc_map[pid].append(eid)
    enc_rows.append({
        "encounter_id": eid, "patient_id": pid,
        "provider_id": random.choice(provider_ids),
        "facility_name": random.choice(FACILITIES),
        "encounter_type": etype,
        "admit_date": admit,
        "discharge_date": admit if etype != "Inpatient" else
                          admit + datetime.timedelta(days=random.randint(1,5)),
        "discharge_disposition": random.choice(["Home","Home","SNF","Home Health"])
                                 if etype == "Inpatient" else None,
        "primary_diagnosis_code": random.choice(DM_ICD10)[0] if is_dm
                                  else random.choice(COMORBID_ICD10)[0],
        "drg_code": str(random.randint(600,700)) if etype == "Inpatient" else None,
        "created_at": datetime.datetime(2023, 1, 1),
    })
enc_pid_map  = {r["encounter_id"]: r["patient_id"]   for r in enc_rows}
enc_prov_map = {r["encounter_id"]: r["provider_id"]  for r in enc_rows}
enc_date_map = {r["encounter_id"]: r["admit_date"]   for r in enc_rows}
enc_type_map = {r["encounter_id"]: r["encounter_type"] for r in enc_rows}
write(enc_rows, "core.encounters")



print("Generating diagnoses...")
diag_rows = []
for eid in enc_ids:
    pid = enc_pid_map[eid]
    prov = enc_prov_map[eid]
    dx_date = enc_date_map[eid]
    is_dm = pid in diabetic_set
    code, desc = random.choice(DM_ICD10) if is_dm else random.choice(COMORBID_ICD10)
    diag_rows.append({
        "diagnosis_id": uid(), "patient_id": pid, "encounter_id": eid, "provider_id": prov,
        "icd10_code": code, "icd10_description": desc, "diagnosis_type": "Primary",
        "diagnosis_date": dx_date,
        "onset_date": dx_date - datetime.timedelta(days=random.randint(30,1000)),
        "resolution_date": None, "status": random.choice(["Chronic","Chronic","Active"]),
    })
    for _ in range(random.randint(1, 2)):
        code2, desc2 = random.choice(COMORBID_ICD10)
        diag_rows.append({
            "diagnosis_id": uid(), "patient_id": pid, "encounter_id": eid, "provider_id": prov,
            "icd10_code": code2, "icd10_description": desc2, "diagnosis_type": "Secondary",
            "diagnosis_date": dx_date,
            "onset_date": dx_date - datetime.timedelta(days=random.randint(30,500)),
            "resolution_date": None, "status": random.choice(["Chronic","Active","Resolved"]),
        })
write(diag_rows, "core.diagnoses")



print("Generating procedures...")
proc_rows = []
for eid in enc_ids[:6000]:
    pid = enc_pid_map[eid]
    prov = enc_prov_map[eid]
    proc_date = enc_date_map[eid]
    is_dm = pid in diabetic_set
    pool = CPTS[:8] if is_dm else CPTS
    for cpt_code, cpt_desc, _ in random.choices(pool, k=random.randint(1, 3)):
        proc_rows.append({
            "procedure_id": uid(), "patient_id": pid, "encounter_id": eid, "provider_id": prov,
            "cpt_code": cpt_code, "cpt_description": cpt_desc, "procedure_date": proc_date,
            "quantity": 1, "modifier": random.choice([None, None, "25", "59", "TC"]),
        })
write(proc_rows, "core.procedures")



print("Generating medications...")
med_rows = []
for pid in patient_ids:
    is_dm = pid in diabetic_set
    pool = MEDS_REF[:8] if is_dm else MEDS_REF
    n = random.randint(2, 5) if is_dm else random.randint(0, 3)
    chosen = random.sample(pool, k=min(n, len(pool)))
    for ndc, brand, generic, drug_class, dose_val, dose_unit, route in chosen:
        rx = rand_date()
        days = random.choice([30, 60, 90])
        refills = random.randint(0, 5)
        enc_list = patient_enc_map[pid]
        med_rows.append({
            "medication_id": uid(), "patient_id": pid,
            "encounter_id": random.choice(enc_list) if enc_list else None,
            "provider_id": random.choice(provider_ids),
            "ndc_code": ndc, "drug_name": brand, "generic_name": generic,
            "drug_class": drug_class, "rx_date": rx, "days_supply": days,
            "quantity": days if route == "Oral" else round(days * float(dose_val) / 30, 1),
            "refills_authorized": refills,
            "refills_remaining": random.randint(0, refills),
            "fill_date": rx + datetime.timedelta(days=random.randint(0, 7)),
            "pharmacy_name": random.choice(PHARMACIES),
            "dose_value": float(dose_val), "dose_unit": dose_unit, "route": route,
        })
write(med_rows, "core.medications")



print("Generating lab results...")
lab_rows = []
for pid in patient_ids:
    is_dm = pid in diabetic_set
    poorly = pid in poorly_controlled_set
    enc_list = patient_enc_map[pid]
    n_visits = min(len(enc_list), random.randint(2, 5))
    for eid in random.sample(enc_list, k=n_visits) if len(enc_list) >= n_visits else enc_list:
        result_date = enc_date_map[eid]
        for loinc, name, unit, ref_lo, ref_hi, mean, std in random.sample(LAB_TESTS, k=random.randint(3,7)):
            if loinc == "4548-4":
                if poorly: val = round(float(np.clip(np.random.normal(10.5, 1.5), 9.1, 14.0)), 1)
                elif is_dm: val = round(float(np.clip(np.random.normal(7.8, 0.8), 6.5, 9.0)), 1)
                else: val = round(float(np.clip(np.random.normal(5.4, 0.5), 4.5, 6.4)), 1)
            elif loinc == "2345-7":
                if poorly: val = round(float(np.clip(np.random.normal(240, 60), 140, 400)), 1)
                elif is_dm: val = round(float(np.clip(np.random.normal(140, 30), 80, 200)), 1)
                else: val = round(float(np.clip(np.random.normal(88, 12), 65, 120)), 1)
            else:
                val = round(float(np.clip(np.random.normal(mean, std), mean-3*std, mean+3*std)), 2)
            flag = "N"
            if val > ref_hi * 1.5: flag = "HH"
            elif val > ref_hi: flag = "H"
            elif val < ref_lo * 0.5: flag = "LL"
            elif val < ref_lo: flag = "L"
            lab_rows.append({
                "result_id": uid(), "patient_id": pid, "encounter_id": eid,
                "provider_id": random.choice(provider_ids),
                "loinc_code": loinc, "test_name": name, "result_date": result_date,
                "result_value": val, "result_unit": unit,
                "reference_range_low": float(ref_lo), "reference_range_high": float(ref_hi),
                "abnormal_flag": flag,
                "lab_name": random.choice(["Quest Diagnostics","LabCorp","In-House Lab"]),
                "specimen_type": random.choice(["Blood","Serum","Urine","Plasma"]),
            })
write(lab_rows, "core.lab_results")



print("Generating wearable daily summaries (~182k rows, may take 60s)...")
wearable_rows = []
date_range = [START + datetime.timedelta(days=i) for i in range(365)]
DEVICES = ["Fitbit Charge 6", "Apple Watch Series 9", "Garmin Venu 3"]

for pid in patient_ids[:500]:
    is_dm = pid in diabetic_set
    device = random.choice(DEVICES)
    dev_id = f"DEV-{uid()[:8].upper()}"
    emp = patient_employer_map[pid]
    base_steps = random.randint(4000, 7500) if is_dm else random.randint(6000, 11000)
    for day in date_range:
        weekend = day.weekday() >= 5
        steps = max(800, int(np.random.normal(base_steps * (1.15 if weekend else 1.0), 1500)))
        rhr = random.randint(62, 90) if is_dm else random.randint(52, 78)
        sleep_min = max(180, int(np.random.normal(380 if is_dm else 430, 50)))
        wearable_rows.append({
            "summary_id": uid(), "patient_id": pid, "device_type": device,
            "device_id": dev_id, "summary_date": day,
            "step_count": steps,
            "active_minutes": max(0, int(steps / 100) + random.randint(-5, 10)),
            "calories_burned": int(steps * 0.05) + random.randint(1400, 1800),
            "distance_km": round(steps * 0.00075, 2),
            "floors_climbed": random.randint(0, 15),
            "resting_heart_rate": rhr,
            "avg_heart_rate": rhr + random.randint(10, 30),
            "max_heart_rate": rhr + random.randint(40, 90),
            "sleep_duration_min": sleep_min,
            "sleep_quality_score": round(float(np.clip(np.random.normal(62 if is_dm else 74, 12), 20, 100)), 1),
            "deep_sleep_min": int(sleep_min * random.uniform(0.12, 0.22)),
            "rem_sleep_min": int(sleep_min * random.uniform(0.18, 0.28)),
            "light_sleep_min": int(sleep_min * random.uniform(0.40, 0.55)),
            "awake_min": random.randint(5, 40),
            "spo2_avg": round(random.uniform(93.0, 99.0), 1),
            "stress_score": round(float(np.clip(np.random.normal(52 if is_dm else 38, 18), 5, 100)), 1),
            "employer_group_id": emp,
        })
write(wearable_rows, "wellness.wearable_daily_summary")



print("Generating CGM readings (~648k rows, may take 90s)...")
cgm_rows = []
CGM_DEVICES = ["Dexcom G7", "Abbott FreeStyle Libre 3"]
cgm_start = datetime.datetime(2024, 1, 1, 0, 0)

for pid in diabetic_ids[:300]:
    poorly = pid in poorly_controlled_set
    device = random.choice(CGM_DEVICES)
    dev_id = f"CGM-{uid()[:8].upper()}"
    for day_offset in range(90):
        for hour in range(24):
            meal_spike = hour in [8, 9, 13, 14, 19, 20]
            if poorly:
                base_g = 220 if meal_spike else 165
                std_g = 45
            else:
                base_g = 155 if meal_spike else 115
                std_g = 25
            glucose = float(np.clip(np.random.normal(base_g, std_g), 55, 400))
            ts = cgm_start + datetime.timedelta(days=day_offset, hours=hour,
                                                minutes=random.randint(0, 4))
            prev = glucose - float(np.random.normal(0, 10))
            delta = glucose - prev
            if delta > 4:   trend = "Rising Rapidly"
            elif delta > 2: trend = "Rising"
            elif delta < -4: trend = "Falling Rapidly"
            elif delta < -2: trend = "Falling"
            else:            trend = "Flat"
            cgm_rows.append({
                "reading_id": uid(), "patient_id": pid,
                "device_id": dev_id, "device_type": device,
                "reading_timestamp": ts,
                "glucose_mg_dl": round(glucose, 2),
                "glucose_mmol_l": round(glucose / 18.0182, 4),
                "trend_arrow": trend,
                "is_calibration": False,
                "signal_strength": random.randint(85, 100),
            })
write(cgm_rows, "wellness.cgm_readings")



print("Generating nutrition logs...")
# (name, cal, protein, carb, fiber, sugar, fat, sat_fat, sodium, gi)
FOODS = [
    ("Oatmeal",           280, 8,  50, 8,  1,  5,  3,  40,  55),
    ("Greek Yogurt",      150, 17, 11, 0,  9,  3,  2,  75,  35),
    ("Grilled Chicken",   330, 40, 0,  0,  0,  7,  2,  120,  0),
    ("Brown Rice",        215, 5,  45, 4,  0,  2,  0,  10,  50),
    ("Side Salad",         80, 3,  8,  3,  4,  4,  0,  90,  15),
    ("Burger & Fries",    900, 32, 95, 6, 22, 42, 15, 980,  80),
    ("Apple",              95, 0,  25, 4, 19, 0,  0,   2,  38),
    ("Soda 12oz",         140, 0,  39, 0, 39, 0,  0,  45,  90),
    ("Almonds 1oz",       165, 6,  6,  4,  1, 14,  1,   0,  15),
    ("Whole Wheat Bread", 130, 5,  24, 3,  3,  2,  0,  230, 69),
    ("Salmon 6oz",        354, 40, 0,  0,  0, 20,  4,  109,  0),
    ("Sweet Potato",      130, 3,  30, 4,  9,  0,  0,  54,  44),
    ("Pizza Slice",       285, 12, 36, 2,  4, 10,  5,  640, 80),
    ("Protein Bar",       210, 20, 22, 5, 12,  7,  3,  230, 55),
    ("Mixed Nuts",        170, 5,  6,  2,  1, 15,  2,   0,  14),
    ("Banana",             90, 1,  23, 3, 12,  0,  0,   1,  51),
    ("Egg White Omelet",  150, 26, 3,  0,  2,  3,  1,  350,  0),
    ("Diet Soda",           0, 0,  0,  0,  0,  0,  0,  40,   0),
]
MEALS = ["Breakfast", "Lunch", "Dinner", "Snack"]
nutrition_rows = []
for pid in patient_ids[:600]:
    for _ in range(random.randint(30, 60)):
        log_date = rand_date()
        for meal in random.sample(MEALS, k=random.randint(2, 4)):
            for food in random.choices(FOODS, k=random.randint(1, 3)):
                name, cal, prot, carb, fib, sug, fat, sfat, sod, gi = food
                s = random.uniform(0.7, 1.4)
                nutrition_rows.append({
                    "log_id": uid(), "patient_id": pid, "log_date": log_date,
                    "meal_type": meal, "food_item": name,
                    "calories": round(cal * s, 1), "protein_g": round(prot * s, 1),
                    "carbohydrates_g": round(carb * s, 1), "fiber_g": round(fib * s, 1),
                    "sugar_g": round(sug * s, 1), "fat_g": round(fat * s, 1),
                    "saturated_fat_g": round(sfat * s, 1), "sodium_mg": round(sod * s, 1),
                    "glycemic_index": gi,
                    "data_source": random.choice(["MyFitnessPal","Cronometer","Manual"]),
                })
write(nutrition_rows, "wellness.nutrition_logs")



print("Generating wellness surveys...")
SURVEY_TYPES = ["PHQ-9", "GAD-7", "SF-36", "Custom Wellness"]
survey_rows = []
for pid in patient_ids[:700]:
    is_dm = pid in diabetic_set
    poorly = pid in poorly_controlled_set
    emp = patient_employer_map[pid]
    for _ in range(random.randint(4, 12)):
        offset = -2 if poorly else (-1 if is_dm else 0)
        survey_rows.append({
            "survey_id": uid(), "patient_id": pid, "survey_date": rand_date(),
            "survey_type": random.choice(SURVEY_TYPES),
            "stress_level":       max(1, min(10, random.randint(3, 9) + offset)),
            "energy_level":       max(1, min(10, random.randint(3, 8) + offset)),
            "sleep_quality":      max(1, min(10, random.randint(3, 8) + offset)),
            "pain_level":         max(0, min(10, random.randint(0, 6) - offset)),
            "mood_score":         max(1, min(10, random.randint(3, 8) + offset)),
            "diet_quality":       max(1, min(10, random.randint(3, 8) + offset)),
            "exercise_adherence": max(1, min(10, random.randint(3, 9) + offset)),
            "medication_adherence": random.random() > (0.25 if poorly else 0.05),
            "employer_group_id": emp, "response_json": None,
        })
write(survey_rows, "wellness.wellness_surveys")



print("Generating claims...")
claim_rows, claim_line_rows, remit_rows = [], [], []
claim_ids_for_denial = []
claim_meta = {}  # cid -> {payer_id, provider_id, patient_id, billed, paid, adj_date, status}

for i, eid in enumerate(enc_ids[:6000]):
    pid = enc_pid_map[eid]
    payer_id = patient_payer_map[pid]
    pname = payer_name_map[payer_id]
    prov_id = enc_prov_map[eid]
    svc_date = enc_date_map[eid]
    submission = svc_date + datetime.timedelta(days=random.randint(1, 30))
    adj_date = submission + datetime.timedelta(days=random.randint(5, 45))

    denial_prob = 0.25 if pname in HIGH_DENIAL_PAYERS else 0.12
    is_denied = random.random() < denial_prob
    claim_status = "Denied" if is_denied else \
                   random.choices(["Paid","Paid","Paid","Pending","Appealed"],
                                  weights=[70, 0, 0, 20, 10])[0]

    line_cpts = random.choices(CPTS, k=random.randint(1, 4))
    total_billed = round(sum(c[2] * random.uniform(0.9, 1.2) for c in line_cpts), 2)
    allowed_pct = random.uniform(0.55, 0.85)
    paid_pct = allowed_pct * random.uniform(0.80, 0.95) if not is_denied else 0.0
    total_allowed = round(total_billed * allowed_pct, 2) if not is_denied else 0.0
    total_paid = round(total_billed * paid_pct, 2)

    cid = uid()
    pos = {"Outpatient":"11","Inpatient":"21","Emergency":"23","Telehealth":"02"}.get(
        enc_type_map[eid], "11")
    claim_rows.append({
        "claim_id": cid, "patient_id": pid, "payer_id": payer_id,
        "provider_id": prov_id, "encounter_id": eid,
        "claim_type": random.choice(["Professional (837P)","Professional (837P)","Institutional (837I)"]),
        "claim_status": claim_status,
        "service_from_date": svc_date, "service_to_date": svc_date,
        "submission_date": submission, "adjudication_date": adj_date,
        "total_billed_amount": total_billed,
        "total_allowed_amount": total_allowed,
        "total_paid_amount": total_paid,
        "total_adjustment_amount": round(total_allowed - total_paid, 2),
        "patient_responsibility": round(total_billed * random.uniform(0.05, 0.20), 2),
        "place_of_service_code": pos,
        "prior_auth_number": f"PA{random.randint(1000000,9999999)}" if random.random() > 0.6 else None,
        "referral_number": None,
    })
    claim_meta[cid] = {"payer_id": payer_id, "provider_id": prov_id,
                       "patient_id": pid, "adj_date": adj_date,
                       "total_billed": total_billed, "total_paid": total_paid,
                       "patient_resp": round(total_billed * random.uniform(0.05, 0.20), 2),
                       "status": claim_status}
    for ln, (cpt_code, cpt_desc, billed_base) in enumerate(line_cpts, 1):
        bl = round(billed_base * random.uniform(0.9, 1.2), 2)
        claim_line_rows.append({
            "line_id": uid(), "claim_id": cid, "line_number": ln,
            "cpt_code": cpt_code, "cpt_description": cpt_desc,
            "icd10_dx_pointer": random.choice(["A","AB","A","B"]),
            "modifier_1": random.choice([None, None, "25", "59"]),
            "modifier_2": None, "service_date": svc_date, "units": 1,
            "billed_amount": bl,
            "allowed_amount": round(bl * allowed_pct, 2) if not is_denied else 0.0,
            "paid_amount": round(bl * paid_pct, 2),
            "adjustment_reason_code": "CO-4" if is_denied else "CO-45",
            "ndc_code": None, "revenue_code": None,
        })
    if is_denied:
        claim_ids_for_denial.append(cid)
    elif claim_status == "Paid":
        remit_rows.append({
            "remittance_id": uid(), "claim_id": cid,
            "payer_id": payer_id, "provider_id": prov_id,
            "check_eft_number": f"EFT{random.randint(10000000,99999999)}",
            "payment_date": adj_date + datetime.timedelta(days=random.randint(3, 14)),
            "payment_method": random.choice(["EFT","EFT","Check"]),
            "payment_amount": total_paid,
            "claim_status_code": "1",
            "total_charge_amount": total_billed,
            "claim_payment_amount": total_paid,
            "patient_responsibility": claim_meta[cid]["patient_resp"],
        })

write(claim_rows, "billing.claims")
write(claim_line_rows, "billing.claim_lines")
write(remit_rows, "billing.remittances")



print("Generating denials...")
denial_rows = []
CATS = ["Authorization","Coding","Eligibility","Duplicate","Timely Filing","Medical Necessity"]
CAT_W = [35, 28, 15, 10, 7, 5]
for cid in claim_ids_for_denial:
    m = claim_meta[cid]
    category = random.choices(CATS, weights=CAT_W)[0]
    carc, carc_desc, rarc = random.choice(DENIAL_DATA[category])
    appealed = random.random() < 0.35
    overturned = appealed and random.random() < 0.45
    appeal_date = m["adj_date"] + datetime.timedelta(days=random.randint(10, 60)) if appealed else None
    pname = payer_name_map[m["payer_id"]]
    denial_rows.append({
        "denial_id": uid(), "claim_id": cid, "line_id": None,
        "payer_id": m["payer_id"], "provider_id": m["provider_id"],
        "patient_id": m["patient_id"],
        "denial_date": m["adj_date"],
        "carc_code": carc, "carc_description": carc_desc,
        "rarc_code": rarc, "rarc_description": f"Remark {rarc} — refer to provider manual",
        "denial_category": category, "denial_amount": m["total_billed"],
        "appeal_status": ("Overturned" if overturned else "Upheld") if appealed else "Not Appealed",
        "appeal_date": appeal_date,
        "appeal_outcome_date": appeal_date + datetime.timedelta(days=random.randint(15,45))
                               if appealed else None,
        "notes": f"Denied by {pname}. Category: {category}.",
    })
write(denial_rows, "billing.denials")



print("Generating authorizations...")
HIGH_COST_CPTS = ["92014","97597","90837","99291","G0108"]
auth_rows = []
for _ in range(2000):
    pid = random.choice(patient_ids)
    payer_id = patient_payer_map[pid]
    pname = payer_name_map[payer_id]
    slow = pname in SLOW_AUTH_PAYERS
    req_date = rand_date()
    turnaround = random.randint(12, 28) if slow else random.randint(1, 10)
    decision_date = req_date + datetime.timedelta(days=turnaround)
    status = random.choices(["Approved","Denied","Pending","Expired","Cancelled"],
                            weights=[60, 15, 15, 7, 3])[0]
    req_units = random.choice([1, 2, 4, 6, 12])
    is_dm = pid in diabetic_set
    auth_rows.append({
        "auth_id": uid(), "patient_id": pid, "payer_id": payer_id,
        "provider_id": random.choice(provider_ids), "claim_id": None,
        "auth_number": f"PA{random.randint(1000000,9999999)}",
        "auth_type": random.choices(["Prior Authorization","Referral","Concurrent Review"],
                                    weights=[70, 20, 10])[0],
        "request_date": req_date, "decision_date": decision_date,
        "effective_date": decision_date,
        "expiration_date": decision_date + datetime.timedelta(days=random.choice([90,180,365])),
        "status": status,
        "cpt_codes": ",".join(random.sample(HIGH_COST_CPTS, k=random.randint(1,2))),
        "icd10_codes": random.choice(DM_ICD10)[0] if is_dm else random.choice(COMORBID_ICD10)[0],
        "requested_units": req_units,
        "approved_units": req_units if status == "Approved" else 0,
        "denial_reason": "Not medically necessary per plan criteria" if status == "Denied" else None,
        "turnaround_days": turnaround,
        "urgency": random.choices(["Routine","Urgent","Emergent"], weights=[75,20,5])[0],
    })
write(auth_rows, "billing.authorizations")



print("Generating payer contracts...")
contract_rows = []
schedule_map = {"Medicare": ("Medicare",100.0), "Medicaid": ("Medicare+X%",75.0),
                "Commercial": ("Medicare+X%",130.0), "Self-Pay": ("Custom",40.0)}
for pr in payer_rows:
    sched, pct = schedule_map.get(pr["payer_type"], ("Custom", 100.0))
    contract_rows.append({
        "contract_id": uid(), "payer_id": pr["payer_id"], "provider_id": None,
        "contract_name": f"{pr['payer_name']} Master Agreement 2023",
        "contract_type": "Fee for Service",
        "effective_date": datetime.date(2023, 1, 1),
        "termination_date": datetime.date(2025, 12, 31),
        "status": "Active",
        "fee_schedule_type": sched,
        "fee_schedule_pct": round(pct + random.uniform(-10, 15), 2),
        "capitation_pmpm": None,
        "clean_claim_days": random.choice([30, 30, 45]),
        "auth_required_cpt_codes": ",".join(random.sample([c[0] for c in CPTS[:8]], k=random.randint(2,5))),
        "specialty_carve_outs": random.choice([None, "Behavioral Health", "Oncology", None]),
        "notes": None,
    })
write(contract_rows, "billing.payer_contracts")



print("\n" + "="*55)
print("DATA GENERATION COMPLETE")
print("="*55)
spark.sql(f"""
  SELECT 'core.patients'                     AS tbl, COUNT(*) AS rows FROM {CATALOG}.core.patients
  UNION ALL SELECT 'core.providers',               COUNT(*) FROM {CATALOG}.core.providers
  UNION ALL SELECT 'core.payers',                  COUNT(*) FROM {CATALOG}.core.payers
  UNION ALL SELECT 'core.encounters',              COUNT(*) FROM {CATALOG}.core.encounters
  UNION ALL SELECT 'core.diagnoses',               COUNT(*) FROM {CATALOG}.core.diagnoses
  UNION ALL SELECT 'core.procedures',              COUNT(*) FROM {CATALOG}.core.procedures
  UNION ALL SELECT 'core.medications',             COUNT(*) FROM {CATALOG}.core.medications
  UNION ALL SELECT 'core.lab_results',             COUNT(*) FROM {CATALOG}.core.lab_results
  UNION ALL SELECT 'wellness.wearable_daily_summary', COUNT(*) FROM {CATALOG}.wellness.wearable_daily_summary
  UNION ALL SELECT 'wellness.cgm_readings',        COUNT(*) FROM {CATALOG}.wellness.cgm_readings
  UNION ALL SELECT 'wellness.nutrition_logs',      COUNT(*) FROM {CATALOG}.wellness.nutrition_logs
  UNION ALL SELECT 'wellness.wellness_surveys',    COUNT(*) FROM {CATALOG}.wellness.wellness_surveys
  UNION ALL SELECT 'billing.claims',               COUNT(*) FROM {CATALOG}.billing.claims
  UNION ALL SELECT 'billing.claim_lines',          COUNT(*) FROM {CATALOG}.billing.claim_lines
  UNION ALL SELECT 'billing.remittances',          COUNT(*) FROM {CATALOG}.billing.remittances
  UNION ALL SELECT 'billing.denials',              COUNT(*) FROM {CATALOG}.billing.denials
  UNION ALL SELECT 'billing.authorizations',       COUNT(*) FROM {CATALOG}.billing.authorizations
  UNION ALL SELECT 'billing.payer_contracts',      COUNT(*) FROM {CATALOG}.billing.payer_contracts
  ORDER BY tbl
""").display()
