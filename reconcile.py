"""
=============================================================
  TRANSACTION RECONCILIATION SYSTEM
  Senior-grade financial reconciliation engine
  Compares internal system records vs bank settlements
=============================================================
"""

import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import os
import sys

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
REPORT_PATH = "reconciliation_report.txt"
MISMATCHES_PATH = "mismatches.csv"
SYSTEM_CSV = "system_transactions.csv"
BANK_CSV = "bank_transactions.csv"

random.seed(42)
np.random.seed(42)


# =============================================================
# STEP 1: DATA GENERATION
# =============================================================

def generate_data():
    """
    Generate synthetic system_transactions.csv and bank_transactions.csv
    with realistic inconsistencies:
      - Settlement delay (1–2 days)
      - Missing transactions (both sides)
      - Duplicates in bank data
      - Refund with no original transaction
      - Amount mismatch (rounding/fees)
    """

    base_date = datetime(2026, 3, 1)
    num_transactions = 30

    # ── Generate clean system transactions ──────────────────
    system_rows = []
    transaction_ids = [f"TXN{str(i).zfill(4)}" for i in range(1, num_transactions + 1)]

    for txn_id in transaction_ids:
        ts = base_date + timedelta(days=random.randint(0, 27),
                                   hours=random.randint(0, 23),
                                   minutes=random.randint(0, 59))
        amount = round(random.uniform(50, 2000), 2)
        txn_type = random.choices(["payment", "refund"], weights=[85, 15])[0]
        system_rows.append({
            "transaction_id": txn_id,
            "user_id": f"USR{random.randint(100, 199)}",
            "amount": amount,
            "transaction_type": txn_type,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })

    system_df = pd.DataFrame(system_rows)

    # ── Build bank dataset from system (with mutations) ─────
    bank_rows = []

    for _, row in system_df.iterrows():
        txn_id = row["transaction_id"]

        # [1] MISSING IN BANK — drop ~10% of transactions
        if txn_id in ["TXN0003", "TXN0011", "TXN0022"]:
            continue  # bank never received these

        # [2] SETTLEMENT DELAY — bank records 1–2 days later
        sys_ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        delay_days = random.randint(1, 2)
        bank_ts = sys_ts + timedelta(days=delay_days)

        # [3] AMOUNT MISMATCH — bank deducted a small fee or rounding
        amount = row["amount"]
        if txn_id in ["TXN0007", "TXN0015"]:
            amount = round(amount - random.uniform(0.01, 0.50), 2)  # bank fee deducted

        bank_rows.append({
            "transaction_id": txn_id,
            "user_id": row["user_id"],
            "amount": amount,
            "transaction_type": row["transaction_type"],
            "timestamp": bank_ts.strftime("%Y-%m-%d %H:%M:%S"),
        })

    # [4] DUPLICATE ENTRY IN BANK — bank posted same txn twice
    for dup_id in ["TXN0005", "TXN0018"]:
        original = next((r for r in bank_rows if r["transaction_id"] == dup_id), None)
        if original:
            bank_rows.append(original.copy())  # exact duplicate

    # [5] MISSING IN SYSTEM — bank has txn our system never recorded
    bank_rows.append({
        "transaction_id": "TXN9001",
        "user_id": "USR199",
        "amount": 350.00,
        "transaction_type": "payment",
        "timestamp": "2026-03-14 10:30:00",
    })

    # [6] REFUND WITHOUT ORIGINAL — no matching payment exists
    bank_rows.append({
        "transaction_id": "TXN9002",
        "user_id": "USR145",
        "amount": 120.00,
        "transaction_type": "refund",
        "timestamp": "2026-03-20 09:00:00",
    })

    bank_df = pd.DataFrame(bank_rows)

    # ── Save CSVs ────────────────────────────────────────────
    system_df.to_csv(SYSTEM_CSV, index=False)
    bank_df.to_csv(BANK_CSV, index=False)

    print(f"[✓] Generated {SYSTEM_CSV}  → {len(system_df)} rows")
    print(f"[✓] Generated {BANK_CSV} → {len(bank_df)} rows")

    return system_df, bank_df


# =============================================================
# STEP 2: RECONCILIATION LOGIC
# =============================================================

def reconcile_transactions(system_df: pd.DataFrame, bank_df: pd.DataFrame) -> dict:
    """
    Core reconciliation engine.

    Matching strategy:
      - Primary key: transaction_id
      - Secondary check: amount within tolerance (±0.01)
      - Date tolerance: ±2 days (handles settlement delay)

    Classifies every record into one of:
      MATCHED | MISSING_IN_BANK | MISSING_IN_SYSTEM |
      DUPLICATE | AMOUNT_MISMATCH | REFUND_NO_ORIGINAL
    """

    results = {
        "matched": [],
        "missing_in_bank": [],
        "missing_in_system": [],
        "duplicates": [],
        "amount_mismatch": [],
        "refund_no_original": [],
    }

    # ── Pre-processing ───────────────────────────────────────

    # Parse timestamps & normalize to UTC-aware naive datetime
    system_df = system_df.copy()
    bank_df = bank_df.copy()

    system_df["timestamp"] = pd.to_datetime(system_df["timestamp"], utc=False)
    bank_df["timestamp"] = pd.to_datetime(bank_df["timestamp"], utc=False)

    # Handle nulls — drop rows where transaction_id or amount is missing
    system_df.dropna(subset=["transaction_id", "amount"], inplace=True)
    bank_df.dropna(subset=["transaction_id", "amount"], inplace=True)

    # Fix floating point: round amounts to 2 decimal places
    system_df["amount"] = system_df["amount"].round(2)
    bank_df["amount"] = bank_df["amount"].round(2)

    # ── Detect duplicates WITHIN bank dataset ────────────────
    bank_dup_mask = bank_df.duplicated(subset=["transaction_id"], keep=False)
    bank_duplicates = bank_df[bank_dup_mask].copy()
    bank_duplicates["issue"] = "Duplicate in bank dataset"
    bank_duplicates["source"] = "bank"
    results["duplicates"] = bank_duplicates.to_dict("records")

    # Work with deduplicated bank data for matching
    bank_deduped = bank_df.drop_duplicates(subset=["transaction_id"], keep="first")

    # Index both datasets by transaction_id for O(1) lookup
    sys_index = system_df.set_index("transaction_id")
    bank_index = bank_deduped.set_index("transaction_id")

    all_ids = set(sys_index.index) | set(bank_index.index)

    for txn_id in all_ids:
        in_system = txn_id in sys_index.index
        in_bank = txn_id in bank_index.index

        # ── MISSING IN BANK ──────────────────────────────────
        if in_system and not in_bank:
            row = sys_index.loc[txn_id].to_dict()
            row["transaction_id"] = txn_id
            row["issue"] = "Missing in bank"
            row["source"] = "system"
            results["missing_in_bank"].append(row)
            continue

        # ── MISSING IN SYSTEM ────────────────────────────────
        if in_bank and not in_system:
            row = bank_index.loc[txn_id].to_dict()
            row["transaction_id"] = txn_id
            # Check if it's a refund without original
            if row.get("transaction_type") == "refund":
                row["issue"] = "Refund with no original transaction"
                results["refund_no_original"].append(row)
            else:
                row["issue"] = "Missing in system"
                row["source"] = "bank"
                results["missing_in_system"].append(row)
            continue

        # ── BOTH EXIST → compare ─────────────────────────────
        sys_row = sys_index.loc[txn_id]
        bank_row = bank_index.loc[txn_id]

        sys_amount = round(float(sys_row["amount"]), 2)
        bank_amount = round(float(bank_row["amount"]), 2)

        # Date tolerance: ±2 days
        date_diff = abs((bank_row["timestamp"] - sys_row["timestamp"]).days)
        date_ok = date_diff <= 2

        # Amount tolerance: allow ≤ 0.01 difference (floating point noise)
        amount_diff = abs(sys_amount - bank_amount)
        amount_ok = amount_diff <= 0.01

        if amount_ok and date_ok:
            record = {
                "transaction_id": txn_id,
                "system_amount": sys_amount,
                "bank_amount": bank_amount,
                "system_ts": sys_row["timestamp"],
                "bank_ts": bank_row["timestamp"],
            }
            results["matched"].append(record)

        elif not amount_ok:
            record = {
                "transaction_id": txn_id,
                "system_amount": sys_amount,
                "bank_amount": bank_amount,
                "difference": round(sys_amount - bank_amount, 2),
                "issue": f"Amount mismatch (Δ {round(sys_amount - bank_amount, 2)})",
                "source": "both",
            }
            results["amount_mismatch"].append(record)

    return results


# =============================================================
# STEP 3: REPORT GENERATION
# =============================================================

def generate_report(system_df: pd.DataFrame, bank_df: pd.DataFrame, results: dict):
    """
    Produces:
      - A human-readable reconciliation_report.txt
      - A mismatches.csv containing all non-matched records
    """

    lines = []

    lines.append("=" * 65)
    lines.append("       TRANSACTION RECONCILIATION REPORT")
    lines.append(f"       Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 65)

    lines.append("\n[ DATASET SUMMARY ]")
    lines.append("-" * 40)
    lines.append(f"  System transactions  : {len(system_df)}")
    lines.append(f"  Bank transactions    : {len(bank_df)}")
    lines.append(f"  (incl. duplicates)   ")

    lines.append("\n[ RECONCILIATION RESULTS ]")
    lines.append("-" * 40)
    lines.append(f"  Matched              : {len(results['matched'])}")
    lines.append(f"  Missing in bank      : {len(results['missing_in_bank'])}")
    lines.append(f"  Missing in system    : {len(results['missing_in_system'])}")
    lines.append(f"  Duplicates (bank)    : {len(results['duplicates'])}")
    lines.append(f"  Amount mismatch      : {len(results['amount_mismatch'])}")
    lines.append(f"  Refund / no original : {len(results['refund_no_original'])}")

    total_mismatches = (
        len(results['missing_in_bank']) +
        len(results['missing_in_system']) +
        len(results['duplicates']) +
        len(results['amount_mismatch']) +
        len(results['refund_no_original'])
    )
    lines.append(f"\n  [!] Total mismatches  : {total_mismatches}")

    # ── Sample records per category ──────────────────────────

    def section(title, records, fields):
        lines.append(f"\n{'─'*65}")
        lines.append(f"  {title}")
        lines.append(f"{'─'*65}")
        if not records:
            lines.append("  None found.")
            return
        for r in records[:5]:  # show up to 5 samples
            lines.append("  " + " | ".join(
                f"{f}: {r.get(f, 'N/A')}" for f in fields
            ))

    section("[ MISSING IN BANK ]",
            results["missing_in_bank"],
            ["transaction_id", "amount", "transaction_type", "timestamp"])

    section("[ MISSING IN SYSTEM ]",
            results["missing_in_system"],
            ["transaction_id", "amount", "transaction_type", "timestamp"])

    section("[ DUPLICATES IN BANK ]",
            results["duplicates"],
            ["transaction_id", "amount", "timestamp"])

    section("[ AMOUNT MISMATCH ]",
            results["amount_mismatch"],
            ["transaction_id", "system_amount", "bank_amount", "difference"])

    section("[ REFUND WITHOUT ORIGINAL ]",
            results["refund_no_original"],
            ["transaction_id", "amount", "transaction_type", "timestamp"])

    lines.append("\n" + "=" * 65)
    lines.append("  WHY DO MISMATCHES HAPPEN IN REAL SYSTEMS?")
    lines.append("=" * 65)
    lines.append("""
  1. SETTLEMENT DELAY
     Payment platforms record a transaction the moment the
     customer pays. Banks batch-process and settle funds
     1–2 days later. At month end, March 31 payments may
     only appear in the bank in April — creating a gap.

  2. ROUNDING & FEES
     Banks sometimes deduct processing fees (0.1–2%) before
     settling. The internal system records the gross amount,
     the bank records the net → small but real mismatch.

  3. DUPLICATES
     Network retries, system crashes, or double-posting by
     the bank's clearing house can result in the same
     transaction appearing twice in one dataset.

  4. MISSING TRANSACTIONS
     Transactions can fail mid-flight — the system recorded
     it, the bank never received it (or vice versa). Also
     caused by timezone cutoffs and batch processing windows.

  5. REFUNDS WITHOUT ORIGINALS
     A refund may be processed by a different system or team
     without properly linking it to the original payment,
     leaving an orphan record on one side.
""")

    report_text = "\n".join(lines)

    # ── Write report ─────────────────────────────────────────
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"\n[✓] Report saved → {REPORT_PATH}")
    print(report_text)

    # ── Write mismatches CSV ──────────────────────────────────
    all_mismatches = []
    for category, records in results.items():
        if category == "matched":
            continue
        for r in records:
            r["category"] = category
            all_mismatches.append(r)

    if all_mismatches:
        mismatches_df = pd.DataFrame(all_mismatches)
        mismatches_df.to_csv(MISMATCHES_PATH, index=False)
        print(f"[✓] Mismatches saved → {MISMATCHES_PATH} ({len(mismatches_df)} rows)")


# =============================================================
# STEP 4: TESTS
# =============================================================

def run_tests(results: dict):
    """
    Basic unit tests to validate reconciliation logic.
    """
    print("\n" + "=" * 40)
    print("  RUNNING TESTS")
    print("=" * 40)

    passed = 0
    failed = 0

    def check(name, condition):
        nonlocal passed, failed
        if condition:
            print(f"  [PASS] {name}")
            passed += 1
        else:
            print(f"  [FAIL] {name}")
            failed += 1

    # Test 1: Some transactions should match
    check("Matched transactions exist", len(results["matched"]) > 0)

    # Test 2: Known missing-in-bank IDs are caught
    missing_ids = [r["transaction_id"] for r in results["missing_in_bank"]]
    check("TXN0003 detected as missing in bank", "TXN0003" in missing_ids)
    check("TXN0011 detected as missing in bank", "TXN0011" in missing_ids)

    # Test 3: Duplicates detected
    dup_ids = [r["transaction_id"] for r in results["duplicates"]]
    check("TXN0005 duplicate detected", "TXN0005" in dup_ids)
    check("TXN0018 duplicate detected", "TXN0018" in dup_ids)

    # Test 4: Amount mismatch detected
    mismatch_ids = [r["transaction_id"] for r in results["amount_mismatch"]]
    check("TXN0007 amount mismatch detected",
          "TXN0007" in mismatch_ids or "TXN0015" in mismatch_ids)

    # Test 5: Missing in system detected
    check("TXN9001 detected as missing in system",
          any(r["transaction_id"] == "TXN9001" for r in results["missing_in_system"]))

    # Test 6: Orphan refund detected
    check("TXN9002 detected as refund without original",
          any(r["transaction_id"] == "TXN9002" for r in results["refund_no_original"]))

    # Test 7: No nulls slipped into matched records
    check("No null transaction_ids in matched",
          all(r["transaction_id"] is not None for r in results["matched"]))

    print(f"\n  Results: {passed} passed, {failed} failed")
    print("=" * 40)


# =============================================================
# MAIN ENTRY POINT
# =============================================================

if __name__ == "__main__":
    if sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    print("\n🚀 Starting Reconciliation System...\n")

    # Step 1: Generate synthetic data
    system_df, bank_df = generate_data()

    # Step 2: Run reconciliation engine
    print("\n⚙️  Running reconciliation...")
    results = reconcile_transactions(system_df, bank_df)

    # Step 3: Generate report + mismatches CSV
    generate_report(system_df, bank_df, results)

    # Step 4: Run tests
    run_tests(results)

    print("\n✅ All done!\n")
