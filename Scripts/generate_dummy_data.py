# scripts/generate_dummy_data.py
"""
Generate dummy Members, Providers and Claims datasets.
Creates both CSV and Parquet files under ../data/csv and ../data/parquet
Designed for quick local development of the Claims & Payments Analytics project.
"""

from faker import Faker
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

# ------- CONFIG -------
OUT_DIR = Path("C:/Users/DEEPTHI/health-finance-claims/Data")   # repo_root/data
CSV_DIR = OUT_DIR / "csv"
PARQUET_DIR = OUT_DIR / "parquet"

MEMBERS_N = 1000
PROVIDERS_N = 200
CLAIMS_N = 5000

SEED = 42
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 10, 31)
# ----------------------

random.seed(SEED)
np.random.seed(SEED)
faker = Faker()
Faker.seed(SEED)

# ensure folders exist
CSV_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# Helper: random date between
def random_date(start, end):
    span = (end - start).days
    return start + timedelta(days=random.randint(0, span), seconds=random.randint(0, 86399))

# ------- MEMBERS -------
genders = ["M", "F", "Other"]
locations = [f"{faker.city()}, {faker.state_abbr()}" for _ in range(200)]

members = []
for i in range(1, MEMBERS_N + 1):
    name = faker.name()
    age = random.randint(0, 90)
    gender = random.choice(genders)
    location = random.choice(locations)
    members.append({
        "member_id": i,
        "name": name,
        "age": age,
        "gender": gender,
        "location": location
    })

df_members = pd.DataFrame(members)

# ------- PROVIDERS -------
specialties = [
    "General Practice", "Cardiology", "Orthopedics", "Dermatology",
    "Oncology", "Neurology", "Psychiatry", "Pediatrics", "OB-GYN",
    "Radiology", "Surgery", "Endocrinology"
]
cities = [faker.city() for _ in range(100)]

providers = []
for i in range(1, PROVIDERS_N + 1):
    name = f"{faker.last_name()} {random.choice(['MD','Clinic','Center','LLP'])}"
    specialty = random.choice(specialties)
    city = random.choice(cities)
    providers.append({
        "provider_id": i,
        "name": name,
        "specialty": specialty,
        "city": city
    })

df_providers = pd.DataFrame(providers)

# ------- CLAIMS -------
diagnoses = [
    "J20", "I10", "E11", "M54", "K21", "N39", "R07", "F41",
    "S72", "C50", "G40", "L40", "H10"
]  # short pseudo-ICD codes for variety

statuses = ["PAID", "DENIED", "PENDING", "REJECTED"]

claims = []
for i in range(1, CLAIMS_N + 1):
    claim_date = random_date(START_DATE, END_DATE)
    member_id = random.randint(1, MEMBERS_N)
    provider_id = random.randint(1, PROVIDERS_N)
    diagnosis = random.choice(diagnoses)
    # amounts: between 100 and 100k
    amount = round(float(np.random.uniform(100.0, 100000.0)), 2)
    status = random.choices(statuses, weights=[0.7, 0.12, 0.12, 0.06], k=1)[0]  # more PAID
    if status == "PAID":
        # paid between 60% and 100% of amount
        paid_pct = round(np.random.uniform(0.6, 1.0), 2)
        paid_amt = round(amount * paid_pct, 2)
    elif status == "PENDING":
        # partial or zero
        paid_amt = round(amount * np.random.choice([0.0, 0.1, 0.2, 0.5]), 2)
    else:  # DENIED or REJECTED
        paid_amt = 0.0

    # simulate a simple turnaround time (days) only for realism
    adjudication_days = np.random.poisson(7)  # avg 7 days
    adjudicated_date = claim_date + timedelta(days=int(adjudication_days)) if status in ["PAID","DENIED","REJECTED"] else pd.NaT

    claims.append({
        "claim_id": i,
        "member_id": member_id,
        "provider_id": provider_id,
        "diagnosis": diagnosis,
        "claim_date": claim_date.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "paid_amt": paid_amt,
        "status": status,
        "adjudicated_date": adjudicated_date.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(adjudicated_date) else "",
        "ingestion_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    })

df_claims = pd.DataFrame(claims)

# Partitioned Parquet writer for CLAIMS
def save_df_partitioned(df: pd.DataFrame, name: str, partition_cols: list):
    pq_path = PARQUET_DIR / name  # directory for partitions
    pq_path.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        pq_path,
        index=False,
        partition_cols=partition_cols,
        engine="pyarrow"
    )
    print(f"Written partitioned parquet: {pq_path} (partitions: {partition_cols})")

# ------- Save files -------
def save_df(df: pd.DataFrame, name: str):
    csv_path = CSV_DIR / f"{name}.csv"
    pq_path = PARQUET_DIR / f"{name}.parquet"
    df.to_csv(csv_path, index=False)
    # to_parquet requires pyarrow or fastparquet; using pyarrow recommended
    df.to_parquet(pq_path, index=False)
    print(f"Written: {csv_path} ({df.shape})")
    print(f"Written: {pq_path} ({df.shape})")
	

# Add partition columns for claims
df_claims["year"] = pd.to_datetime(df_claims["claim_date"]).dt.year
df_claims["month"] = pd.to_datetime(df_claims["claim_date"]).dt.month

save_df(df_members, "members")
save_df(df_providers, "providers")
#save_df(df_claims, "claims")
df_claims.to_csv(CSV_DIR / "claims.csv", index=False)

save_df_partitioned(
    df_claims,
    name="claims",
    partition_cols=["year", "month"]
)
print("DONE: Dummy data generated.")

