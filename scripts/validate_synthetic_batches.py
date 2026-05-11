from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

BATCH_001 = DATA_DIR / "batch_001"
BATCH_002 = DATA_DIR / "batch_002"
BATCH_003 = DATA_DIR / "batch_003"


def read_csv(batch_dir: Path, name: str) -> pd.DataFrame:
    return pd.read_csv(batch_dir / f"{name}.csv")


def print_section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def validate_row_counts() -> None:
    print_section("ROW COUNT VALIDATION")

    expected = {
        "batch_001": {
            "accounts": 4000,
            "claims": 12000,
            "remittances": 5000,
            "workqueue_actions": 15000,
            "collectors": 20,
            "payers": 15,
            "facilities": 8,
        },
        "batch_002": {
            "accounts": 700,   # 400 changed + 300 new
            "claims": 1750,    # 250 changed + 1500 new
            "remittances": 2500,
            "workqueue_actions": 5000,
        },
        "batch_003": {
            "accounts": 500,   # 300 changed + 200 new
            "claims": 1350,    # 150 changed + 1200 new
            "remittances": 2200,
            "workqueue_actions": 4000,
        },
    }

    for batch_name, entities in expected.items():
        batch_dir = DATA_DIR / batch_name
        print(f"\n{batch_name}:")
        for entity, expected_count in entities.items():
            df = read_csv(batch_dir, entity)
            actual_count = len(df)
            status = "OK" if actual_count == expected_count else "MISMATCH"
            print(
                f"  {entity:20s} expected={expected_count:6d} actual={actual_count:6d} status={status}"
            )


def validate_account_lifecycle(account_id: int) -> None:
    print_section(f"ACCOUNT LIFECYCLE CHECK: account_id={account_id}")

    for batch_name in ["batch_001", "batch_002", "batch_003"]:
        batch_dir = DATA_DIR / batch_name
        if not (batch_dir / "accounts.csv").exists():
            continue

        df = read_csv(batch_dir, "accounts")
        row = df[df["account_id"] == account_id]

        if row.empty:
            print(f"{batch_name}: NOT PRESENT")
        else:
            print(f"{batch_name}:")
            print(
                row[
                    [
                        "account_id",
                        "collector_id",
                        "queue_name",
                        "current_balance",
                        "updated_at",
                        "load_ts",
                        "batch_id",
                    ]
                ].to_string(index=False)
            )


def validate_claim_lifecycle(claim_id: int) -> None:
    print_section(f"CLAIM LIFECYCLE CHECK: claim_id={claim_id}")

    for batch_name in ["batch_001", "batch_002", "batch_003"]:
        batch_dir = DATA_DIR / batch_name
        if not (batch_dir / "claims.csv").exists():
            continue

        df = read_csv(batch_dir, "claims")
        row = df[df["claim_id"] == claim_id]

        if row.empty:
            print(f"{batch_name}: NOT PRESENT")
        else:
            print(f"{batch_name}:")
            print(
                row[
                    [
                        "claim_id",
                        "account_id",
                        "service_date",
                        "claim_status",
                        "billed_amount",
                        "load_ts",
                        "batch_id",
                    ]
                ].to_string(index=False)
            )


def validate_late_remits(target_claim_ids: list[int]) -> None:
    print_section("LATE-ARRIVING REMITTANCE CHECK")

    df = read_csv(BATCH_003, "remittances")
    rows = df[df["claim_id"].isin(target_claim_ids)].copy()

    if rows.empty:
        print("No matching late-arriving remittances found.")
        return

    rows = rows[
        [
            "remit_id",
            "claim_id",
            "payment_date",
            "paid_amount",
            "adjustment_amount",
            "load_ts",
            "batch_id",
        ]
    ].sort_values(["claim_id", "remit_id"])

    print(rows.to_string(index=False))


def validate_new_ids() -> None:
    print_section("NEW ID CHECKS")

    batch_001_accounts = read_csv(BATCH_001, "accounts")
    batch_002_accounts = read_csv(BATCH_002, "accounts")
    batch_003_accounts = read_csv(BATCH_003, "accounts")

    batch_001_claims = read_csv(BATCH_001, "claims")
    batch_002_claims = read_csv(BATCH_002, "claims")
    batch_003_claims = read_csv(BATCH_003, "claims")

    new_accounts_b2 = sorted(
        set(batch_002_accounts["account_id"]) - set(batch_001_accounts["account_id"])
    )
    new_accounts_b3 = sorted(
        set(batch_003_accounts["account_id"])
        - set(batch_001_accounts["account_id"])
        - set(batch_002_accounts["account_id"])
    )

    new_claims_b2 = sorted(
        set(batch_002_claims["claim_id"]) - set(batch_001_claims["claim_id"])
    )
    new_claims_b3 = sorted(
        set(batch_003_claims["claim_id"])
        - set(batch_001_claims["claim_id"])
        - set(batch_002_claims["claim_id"])
    )

    print(f"New accounts in batch_002: count={len(new_accounts_b2)} sample={new_accounts_b2[:10]}")
    print(f"New accounts in batch_003: count={len(new_accounts_b3)} sample={new_accounts_b3[:10]}")
    print(f"New claims   in batch_002: count={len(new_claims_b2)} sample={new_claims_b2[:10]}")
    print(f"New claims   in batch_003: count={len(new_claims_b3)} sample={new_claims_b3[:10]}")


def validate_expected_business_scenarios() -> None:
    print_section("EXPECTED BUSINESS SCENARIOS")

    print("Scenario A: account 1005 should appear across multiple batches with changed assignment details.")
    print("Scenario B: claim 2008 should evolve across batches.")
    print("Scenario C: claim 2015 should show a late-arriving remit in batch_003.")
    print("Scenario D: dynamic-table candidate input (workqueue_actions) should have increasing action batches.")


def validate_action_counts() -> None:
    print_section("WORKQUEUE ACTION BATCH CHECK")

    for batch_name in ["batch_001", "batch_002", "batch_003"]:
        df = read_csv(DATA_DIR / batch_name, "workqueue_actions")
        print(f"{batch_name}: action_count={len(df)}")


def main() -> None:
    validate_row_counts()
    validate_new_ids()

    validate_account_lifecycle(1005)
    validate_account_lifecycle(1010)
    validate_account_lifecycle(1015)

    validate_claim_lifecycle(2008)
    validate_claim_lifecycle(2015)
    validate_claim_lifecycle(2022)

    validate_late_remits([2008, 2015, 2022, 2030, 2040])
    validate_action_counts()
    validate_expected_business_scenarios()


if __name__ == "__main__":
    main()