from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
import random

import pandas as pd
from faker import Faker


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BatchConfig:
    batch_id: str
    load_date: date


RANDOM_SEED = 42
fake = Faker()
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

BATCHES = [
    BatchConfig(batch_id="batch_001", load_date=date(2026, 1, 5)),
    BatchConfig(batch_id="batch_002", load_date=date(2026, 1, 12)),
    BatchConfig(batch_id="batch_003", load_date=date(2026, 1, 19)),
]

NUM_FACILITIES = 8
NUM_PAYERS = 15
NUM_COLLECTORS = 20

BATCH_001_ACCOUNT_COUNT = 4000
BATCH_001_CLAIM_COUNT = 12000
BATCH_001_REMIT_COUNT = 5000
BATCH_001_ACTION_COUNT = 15000

BATCH_002_NEW_ACCOUNTS = 300
BATCH_002_CHANGED_ACCOUNTS = 400
BATCH_002_NEW_CLAIMS = 1500
BATCH_002_CHANGED_CLAIMS = 250
BATCH_002_REMITS = 2500
BATCH_002_ACTIONS = 5000

BATCH_003_NEW_ACCOUNTS = 200
BATCH_003_CHANGED_ACCOUNTS = 300
BATCH_003_NEW_CLAIMS = 1200
BATCH_003_CHANGED_CLAIMS = 150
BATCH_003_REMITS = 2200
BATCH_003_ACTIONS = 4000


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for batch in BATCHES:
        (DATA_DIR / batch.batch_id).mkdir(parents=True, exist_ok=True)


def ts_for_load(load_date: date, hour: int = 8) -> datetime:
    return datetime.combine(load_date, datetime.min.time()) + timedelta(hours=hour)


def csv_name(entity: str, batch_id: str) -> str:
    return f"{entity}_{batch_id}.csv"


def write_csv(df: pd.DataFrame, batch_id: str, entity: str) -> None:
    output_path = DATA_DIR / batch_id / f"{entity}.csv"
    df.to_csv(output_path, index=False)


def pick_ids(start: int, count: int) -> list[int]:
    return list(range(start, start + count))


# -----------------------------------------------------------------------------
# REFERENCE DATA
# -----------------------------------------------------------------------------

def build_facilities() -> pd.DataFrame:
    rows = []
    for facility_id in range(1, NUM_FACILITIES + 1):
        rows.append(
            {
                "facility_id": facility_id,
                "facility_name": f"{fake.city()} Medical Center",
                "state": fake.state_abbr(),
                "region": random.choice(["East", "Central", "West"]),
            }
        )
    return pd.DataFrame(rows)


def build_payers() -> pd.DataFrame:
    rows = []
    for payer_id in range(1, NUM_PAYERS + 1):
        rows.append(
            {
                "payer_id": payer_id,
                "payer_name": f"{fake.company()} Health",
                "payer_category": random.choice(
                    ["Commercial", "Medicare", "Medicaid", "Self Pay"]
                ),
                "contract_type": random.choice(
                    ["Standard", "Value-Based", "Capitated"]
                ),
            }
        )
    return pd.DataFrame(rows)


def build_collectors() -> pd.DataFrame:
    rows = []
    for collector_id in range(1, NUM_COLLECTORS + 1):
        rows.append(
            {
                "collector_id": collector_id,
                "collector_name": fake.name(),
                "manager_name": fake.name(),
                "region": random.choice(["East", "Central", "West"]),
                "active_flag": random.choice(["Y", "Y", "Y", "N"]),
            }
        )
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# BATCH 001 BASELINE
# -----------------------------------------------------------------------------

def build_batch_001_accounts(batch: BatchConfig) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 8)
    for account_id in pick_ids(1001, BATCH_001_ACCOUNT_COUNT):
        rows.append(
            {
                "account_id": account_id,
                "patient_id": 500000 + account_id,
                "facility_id": random.randint(1, NUM_FACILITIES),
                "current_balance": round(random.uniform(50, 25000), 2),
                "assignment_status": random.choice(["Assigned", "Unassigned", "Escalated"]),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "queue_name": random.choice(["Early Out", "Denials", "Follow Up", "High Balance"]),
                "updated_at": load_ts,
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("accounts", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_001_claims(batch: BatchConfig, account_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 8)
    for claim_id in pick_ids(2001, BATCH_001_CLAIM_COUNT):
        service_days_ago = random.randint(0, 120)
        rows.append(
            {
                "claim_id": claim_id,
                "account_id": random.choice(account_ids),
                "facility_id": random.randint(1, NUM_FACILITIES),
                "payer_id": random.randint(1, NUM_PAYERS),
                "service_date": batch.load_date - timedelta(days=service_days_ago),
                "billed_amount": round(random.uniform(100, 30000), 2),
                "claim_status": random.choice(["OPEN", "PENDING", "PAID", "DENIED"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("claims", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_001_remittances(batch: BatchConfig, claim_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 9)
    remit_ids = pick_ids(9001, BATCH_001_REMIT_COUNT)
    sampled_claim_ids = random.sample(claim_ids, BATCH_001_REMIT_COUNT)

    for remit_id, claim_id in zip(remit_ids, sampled_claim_ids):
        payment_days_ago = random.randint(0, 60)
        rows.append(
            {
                "remit_id": remit_id,
                "claim_id": claim_id,
                "payment_date": batch.load_date - timedelta(days=payment_days_ago),
                "paid_amount": round(random.uniform(0, 18000), 2),
                "adjustment_amount": round(random.uniform(0, 5000), 2),
                "carc_code": random.choice(["45", "96", "197", "29", "B7", ""]),
                "rarc_code": random.choice(["N115", "M15", "N130", "MA01", ""]),
                "remit_status": random.choice(["POSTED", "PENDING", "REVERSED"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("remittances", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_001_actions(batch: BatchConfig, account_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 10)
    for action_id in pick_ids(30001, BATCH_001_ACTION_COUNT):
        rows.append(
            {
                "action_id": action_id,
                "account_id": random.choice(account_ids),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "action_date": batch.load_date - timedelta(days=random.randint(0, 30)),
                "action_type": random.choice(["CALL", "NOTE", "REVIEW", "ESCALATE"]),
                "note_count": random.randint(0, 4),
                "worked_flag": random.choice(["Y", "N"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("workqueue_actions", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# DELTA BATCH MUTATIONS
# -----------------------------------------------------------------------------

def build_batch_002_accounts(batch: BatchConfig, base_accounts: pd.DataFrame) -> pd.DataFrame:
    load_ts = ts_for_load(batch.load_date, 8)

    changed_ids = pick_ids(1001, BATCH_002_CHANGED_ACCOUNTS)
    changed = base_accounts[base_accounts["account_id"].isin(changed_ids)].copy()

    # deterministic mutations
    changed.loc[:, "collector_id"] = changed["collector_id"].apply(lambda x: ((x + 3 - 1) % NUM_COLLECTORS) + 1)
    changed.loc[:, "queue_name"] = changed["queue_name"].replace(
        {
            "Early Out": "Follow Up",
            "Follow Up": "Denials",
            "Denials": "High Balance",
            "High Balance": "Follow Up",
        }
    )
    changed.loc[:, "current_balance"] = (changed["current_balance"] * 0.85).round(2)
    changed.loc[:, "updated_at"] = load_ts
    changed.loc[:, "load_ts"] = load_ts
    changed.loc[:, "batch_id"] = batch.batch_id
    changed.loc[:, "src_file_name"] = csv_name("accounts", batch.batch_id)

    new_rows = []
    for account_id in pick_ids(1001 + BATCH_001_ACCOUNT_COUNT, BATCH_002_NEW_ACCOUNTS):
        new_rows.append(
            {
                "account_id": account_id,
                "patient_id": 500000 + account_id,
                "facility_id": random.randint(1, NUM_FACILITIES),
                "current_balance": round(random.uniform(50, 25000), 2),
                "assignment_status": random.choice(["Assigned", "Unassigned", "Escalated"]),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "queue_name": random.choice(["Early Out", "Denials", "Follow Up", "High Balance"]),
                "updated_at": load_ts,
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("accounts", batch.batch_id),
            }
        )

    return pd.concat([changed, pd.DataFrame(new_rows)], ignore_index=True)


def build_batch_002_claims(
    batch: BatchConfig,
    base_claims: pd.DataFrame,
    all_account_ids: list[int],
) -> pd.DataFrame:
    load_ts = ts_for_load(batch.load_date, 8)

    changed_ids = pick_ids(2001, BATCH_002_CHANGED_CLAIMS)
    changed = base_claims[base_claims["claim_id"].isin(changed_ids)].copy()

    # deterministic status changes
    first_half = changed.index[: len(changed) // 2]
    second_half = changed.index[len(changed) // 2 :]

    changed.loc[first_half, "claim_status"] = "DENIED"
    changed.loc[second_half, "claim_status"] = "PAID"
    changed.loc[:, "load_ts"] = load_ts
    changed.loc[:, "batch_id"] = batch.batch_id
    changed.loc[:, "src_file_name"] = csv_name("claims", batch.batch_id)

    new_rows = []
    new_claim_start = 2001 + BATCH_001_CLAIM_COUNT
    for claim_id in pick_ids(new_claim_start, BATCH_002_NEW_CLAIMS):
        service_days_ago = random.randint(0, 30)
        new_rows.append(
            {
                "claim_id": claim_id,
                "account_id": random.choice(all_account_ids),
                "facility_id": random.randint(1, NUM_FACILITIES),
                "payer_id": random.randint(1, NUM_PAYERS),
                "service_date": batch.load_date - timedelta(days=service_days_ago),
                "billed_amount": round(random.uniform(100, 30000), 2),
                "claim_status": random.choice(["OPEN", "PENDING", "PAID"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("claims", batch.batch_id),
            }
        )

    return pd.concat([changed, pd.DataFrame(new_rows)], ignore_index=True)


def build_batch_002_remittances(batch: BatchConfig, eligible_claim_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 9)
    for remit_id in pick_ids(9001 + BATCH_001_REMIT_COUNT, BATCH_002_REMITS):
        rows.append(
            {
                "remit_id": remit_id,
                "claim_id": random.choice(eligible_claim_ids),
                "payment_date": batch.load_date - timedelta(days=random.randint(0, 15)),
                "paid_amount": round(random.uniform(0, 18000), 2),
                "adjustment_amount": round(random.uniform(0, 5000), 2),
                "carc_code": random.choice(["45", "96", "197", "29", "B7", ""]),
                "rarc_code": random.choice(["N115", "M15", "N130", "MA01", ""]),
                "remit_status": random.choice(["POSTED", "PENDING", "REVERSED"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("remittances", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_002_actions(batch: BatchConfig, all_account_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 10)
    start_action_id = 30001 + BATCH_001_ACTION_COUNT
    for action_id in pick_ids(start_action_id, BATCH_002_ACTIONS):
        rows.append(
            {
                "action_id": action_id,
                "account_id": random.choice(all_account_ids),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "action_date": batch.load_date - timedelta(days=random.randint(0, 7)),
                "action_type": random.choice(["CALL", "NOTE", "REVIEW", "ESCALATE"]),
                "note_count": random.randint(0, 4),
                "worked_flag": random.choice(["Y", "N"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("workqueue_actions", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_003_accounts(batch: BatchConfig, base_accounts: pd.DataFrame) -> pd.DataFrame:
    load_ts = ts_for_load(batch.load_date, 8)

    changed_ids = pick_ids(1001, BATCH_003_CHANGED_ACCOUNTS)
    changed = base_accounts[base_accounts["account_id"].isin(changed_ids)].copy()

    changed.loc[:, "collector_id"] = changed["collector_id"].apply(lambda x: ((x + 5 - 1) % NUM_COLLECTORS) + 1)
    changed.loc[:, "current_balance"] = (changed["current_balance"] * 0.65).round(2)
    changed.loc[:, "queue_name"] = "Denials"

    # late-arriving account correction for a small subset
    late_ids = [1005, 1010, 1015]
    changed.loc[changed["account_id"].isin(late_ids), "updated_at"] = ts_for_load(date(2026, 1, 10), 12)
    changed.loc[~changed["account_id"].isin(late_ids), "updated_at"] = load_ts

    changed.loc[:, "load_ts"] = load_ts
    changed.loc[:, "batch_id"] = batch.batch_id
    changed.loc[:, "src_file_name"] = csv_name("accounts", batch.batch_id)

    new_rows = []
    start_new = 1001 + BATCH_001_ACCOUNT_COUNT + BATCH_002_NEW_ACCOUNTS
    for account_id in pick_ids(start_new, BATCH_003_NEW_ACCOUNTS):
        new_rows.append(
            {
                "account_id": account_id,
                "patient_id": 500000 + account_id,
                "facility_id": random.randint(1, NUM_FACILITIES),
                "current_balance": round(random.uniform(50, 25000), 2),
                "assignment_status": random.choice(["Assigned", "Unassigned", "Escalated"]),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "queue_name": random.choice(["Early Out", "Denials", "Follow Up", "High Balance"]),
                "updated_at": load_ts,
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("accounts", batch.batch_id),
            }
        )

    return pd.concat([changed, pd.DataFrame(new_rows)], ignore_index=True)


def build_batch_003_claims(
    batch: BatchConfig,
    prior_claims: pd.DataFrame,
    all_account_ids: list[int],
) -> pd.DataFrame:
    load_ts = ts_for_load(batch.load_date, 8)

    changed_ids = pick_ids(2001, BATCH_003_CHANGED_CLAIMS)
    changed = prior_claims[prior_claims["claim_id"].isin(changed_ids)].copy()

    changed.loc[:, "claim_status"] = "PAID"
    changed.loc[:, "load_ts"] = load_ts
    changed.loc[:, "batch_id"] = batch.batch_id
    changed.loc[:, "src_file_name"] = csv_name("claims", batch.batch_id)

    # intentionally late service_date subset
    late_service_ids = [2008, 2015, 2022]
    changed.loc[changed["claim_id"].isin(late_service_ids), "service_date"] = date(2026, 1, 4)

    new_rows = []
    start_new = 2001 + BATCH_001_CLAIM_COUNT + BATCH_002_NEW_CLAIMS
    for claim_id in pick_ids(start_new, BATCH_003_NEW_CLAIMS):
        new_rows.append(
            {
                "claim_id": claim_id,
                "account_id": random.choice(all_account_ids),
                "facility_id": random.randint(1, NUM_FACILITIES),
                "payer_id": random.randint(1, NUM_PAYERS),
                "service_date": batch.load_date - timedelta(days=random.randint(0, 20)),
                "billed_amount": round(random.uniform(100, 30000), 2),
                "claim_status": random.choice(["OPEN", "PENDING", "PAID"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("claims", batch.batch_id),
            }
        )

    return pd.concat([changed, pd.DataFrame(new_rows)], ignore_index=True)


def build_batch_003_remittances(batch: BatchConfig, prior_claim_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 9)

    late_claim_ids = [2008, 2015, 2022, 2030, 2040]
    new_remit_start = 9001 + BATCH_001_REMIT_COUNT + BATCH_002_REMITS

    all_targets = late_claim_ids + random.sample(prior_claim_ids, BATCH_003_REMITS - len(late_claim_ids))

    for remit_id, claim_id in zip(pick_ids(new_remit_start, BATCH_003_REMITS), all_targets):
        payment_date = batch.load_date - timedelta(days=random.randint(0, 12))
        if claim_id in late_claim_ids:
            payment_date = date(2026, 1, 7)

        rows.append(
            {
                "remit_id": remit_id,
                "claim_id": claim_id,
                "payment_date": payment_date,
                "paid_amount": round(random.uniform(0, 18000), 2),
                "adjustment_amount": round(random.uniform(0, 5000), 2),
                "carc_code": random.choice(["45", "96", "197", "29", "B7", ""]),
                "rarc_code": random.choice(["N115", "M15", "N130", "MA01", ""]),
                "remit_status": random.choice(["POSTED", "PENDING", "REVERSED"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("remittances", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


def build_batch_003_actions(batch: BatchConfig, all_account_ids: list[int]) -> pd.DataFrame:
    rows = []
    load_ts = ts_for_load(batch.load_date, 10)
    start_action_id = 30001 + BATCH_001_ACTION_COUNT + BATCH_002_ACTIONS
    for action_id in pick_ids(start_action_id, BATCH_003_ACTIONS):
        rows.append(
            {
                "action_id": action_id,
                "account_id": random.choice(all_account_ids),
                "collector_id": random.randint(1, NUM_COLLECTORS),
                "action_date": batch.load_date - timedelta(days=random.randint(0, 5)),
                "action_type": random.choice(["CALL", "NOTE", "REVIEW", "ESCALATE"]),
                "note_count": random.randint(0, 4),
                "worked_flag": random.choice(["Y", "N"]),
                "load_ts": load_ts,
                "batch_id": batch.batch_id,
                "src_file_name": csv_name("workqueue_actions", batch.batch_id),
            }
        )
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main() -> None:
    ensure_dirs()

    batch_001, batch_002, batch_003 = BATCHES

    facilities = build_facilities()
    payers = build_payers()
    collectors = build_collectors()

    batch_001_accounts = build_batch_001_accounts(batch_001)
    batch_001_claims = build_batch_001_claims(batch_001, batch_001_accounts["account_id"].tolist())
    batch_001_remits = build_batch_001_remittances(batch_001, batch_001_claims["claim_id"].tolist())
    batch_001_actions = build_batch_001_actions(batch_001, batch_001_accounts["account_id"].tolist())

    batch_002_accounts = build_batch_002_accounts(batch_002, batch_001_accounts)
    all_account_ids_for_batch_002 = sorted(
        set(batch_001_accounts["account_id"]).union(set(batch_002_accounts["account_id"]))
    )
    batch_002_claims = build_batch_002_claims(batch_002, batch_001_claims, all_account_ids_for_batch_002)
    all_claim_ids_for_batch_002 = sorted(
        set(batch_001_claims["claim_id"]).union(set(batch_002_claims["claim_id"]))
    )
    batch_002_remits = build_batch_002_remittances(batch_002, all_claim_ids_for_batch_002)
    batch_002_actions = build_batch_002_actions(batch_002, all_account_ids_for_batch_002)

    batch_003_accounts = build_batch_003_accounts(batch_003, pd.concat([batch_001_accounts, batch_002_accounts], ignore_index=True).drop_duplicates(subset=["account_id"], keep="last"))
    all_account_ids_for_batch_003 = sorted(
        set(all_account_ids_for_batch_002).union(set(batch_003_accounts["account_id"]))
    )
    latest_claims_before_batch_003 = pd.concat([batch_001_claims, batch_002_claims], ignore_index=True).drop_duplicates(subset=["claim_id"], keep="last")
    batch_003_claims = build_batch_003_claims(batch_003, latest_claims_before_batch_003, all_account_ids_for_batch_003)
    all_claim_ids_for_batch_003 = sorted(
        set(all_claim_ids_for_batch_002).union(set(batch_003_claims["claim_id"]))
    )
    batch_003_remits = build_batch_003_remittances(batch_003, all_claim_ids_for_batch_003)
    batch_003_actions = build_batch_003_actions(batch_003, all_account_ids_for_batch_003)

    # Write batch 001
    write_csv(facilities, batch_001.batch_id, "facilities")
    write_csv(payers, batch_001.batch_id, "payers")
    write_csv(collectors, batch_001.batch_id, "collectors")
    write_csv(batch_001_accounts, batch_001.batch_id, "accounts")
    write_csv(batch_001_claims, batch_001.batch_id, "claims")
    write_csv(batch_001_remits, batch_001.batch_id, "remittances")
    write_csv(batch_001_actions, batch_001.batch_id, "workqueue_actions")

    # Write batch 002
    write_csv(batch_002_accounts, batch_002.batch_id, "accounts")
    write_csv(batch_002_claims, batch_002.batch_id, "claims")
    write_csv(batch_002_remits, batch_002.batch_id, "remittances")
    write_csv(batch_002_actions, batch_002.batch_id, "workqueue_actions")

    # Write batch 003
    write_csv(batch_003_accounts, batch_003.batch_id, "accounts")
    write_csv(batch_003_claims, batch_003.batch_id, "claims")
    write_csv(batch_003_remits, batch_003.batch_id, "remittances")
    write_csv(batch_003_actions, batch_003.batch_id, "workqueue_actions")

    print("Synthetic batch files created successfully.")
    print(f"Output directory: {DATA_DIR}")


if __name__ == "__main__":
    main()