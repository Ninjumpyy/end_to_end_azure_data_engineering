import random
from datetime import datetime, timedelta, timezone
import pandas as pd

# transactions generator

def generate_transactions(accounts_df: pd.DataFrame, merchants_df: pd.DataFrame, counterparties_df: pd.DataFrame,
                          start_date: str = "2023-01-01", end_date: str = None, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)

    # date window
    start_dt = pd.Timestamp(start_date, tz="UTC")
    if end_date is None:
        end_dt = pd.Timestamp(datetime.utcnow().replace(tzinfo=timezone.utc).date(), tz="UTC")
    else:
        end_dt = pd.Timestamp(end_date, tz="UTC")
    if end_dt < start_dt:
        raise ValueError("end_date must be >= start_date")

    # normalize account dates
    a = accounts_df.copy()
    a["opened_at"] = pd.to_datetime(a["opened_at"], utc=True, errors="raise")
    a["closed_at"] = pd.to_datetime(a["closed_at"], utc=True, errors="coerce")

    # --- hard requirements ---
    if merchants_df.empty:
        raise ValueError("merchants_df is empty: required for CARD_PURCHASE transactions")
    if counterparties_df.empty:
        raise ValueError("counterparties_df is empty: required for TRANSFER transactions")

    # top merchants subset
    merchant_ids = merchants_df["merchant_id"].tolist()

    n_m = len(merchant_ids)
    popularity = [1.0 / (i + 1) for i in range(n_m)]  # 1, 1/2, 1/3...
    top_k = min(n_m, max(25, int(0.10 * n_m)))
    top_merchants = merchant_ids[:top_k]
    top_weights = popularity[:top_k]

    def pick_merchant_id() -> str:
        # 88% from top merchants, 12% from full set
        if rng.random() < 0.88:
            return rng.choices(top_merchants, weights=top_weights, k=1)[0]
        return rng.choices(merchant_ids, weights=popularity, k=1)[0]

    # Counterparties
    cp = counterparties_df.copy()

    if "counterparty_type" not in cp.columns:
        raise ValueError("counterparties_df must contain counterparty_type")

    employers = cp.loc[cp["counterparty_type"] == "EMPLOYER", "counterparty_id"].tolist()
    landlords = cp.loc[cp["counterparty_type"] == "LANDLORD", "counterparty_id"].tolist()
    personals = cp.loc[cp["counterparty_type"] == "PERSONAL", "counterparty_id"].tolist()
    all_cp_ids = cp["counterparty_id"].tolist()

    def build_customer_cp_pool(customer_id: str) -> dict:
        pool = {"employer": None, "landlord": None, "personal": []}

        if employers:
            pool["employer"] = rng.choice(employers)
        if landlords:
            pool["landlord"] = rng.choice(landlords)

        if personals:
            k = rng.choices([1, 2], weights=[70, 30], k=1)[0]
            chosen = []
            for _ in range(min(k, len(personals))):
                cand = rng.choice(personals)
                if cand not in chosen:
                    chosen.append(cand)
            pool["personal"] = chosen

        # fallback if any bucket is missing
        if pool["employer"] is None and all_cp_ids:
            pool["employer"] = rng.choice(all_cp_ids)
        if pool["landlord"] is None and all_cp_ids:
            pool["landlord"] = rng.choice(all_cp_ids)
        if not pool["personal"] and all_cp_ids:
            pool["personal"] = [rng.choice(all_cp_ids)]

        return pool

    customer_pool = {}
    if "customer_id" not in a.columns:
        # accounts generator includes customer_id :contentReference[oaicite:1]{index=1}
        raise ValueError("accounts_df must contain customer_id to build per-customer counterparty pools")

    for cid in a["customer_id"].unique():
        customer_pool[cid] = build_customer_cp_pool(cid)

    def pick_counterparty_id(customer_id: str, direction: str) -> str:
        pool = customer_pool[customer_id]

        # one-off probability
        if rng.random() < 0.25:
            return rng.choice(all_cp_ids)

        # repeated pool selection
        if direction == "IN":
            if rng.random() < 0.80 and pool["employer"]:
                return pool["employer"]
            return rng.choice(pool["personal"])

        # OUT: rent/regular payments/personal transfers out
        r = rng.random()
        if r < 0.55 and pool["landlord"]:
            return pool["landlord"]
        return rng.choice(pool["personal"])

    # ---------- txn config ----------
    txn_types = ["CARD_PURCHASE", "TRANSFER", "ATM_WITHDRAWAL", "BANK_FEE", "INTEREST"]
    txn_type_w = [65.0, 22.5, 7.5, 2.5, 1.5]

    volume_buckets = ["LOW", "NORMAL", "HIGH"]
    volume_w = [40, 50, 10]

    def sample_monthly_txn_count() -> int:
        bucket = rng.choices(volume_buckets, weights=volume_w, k=1)[0]
        if bucket == "LOW":
            return rng.randint(0, 5)
        if bucket == "NORMAL":
            return rng.randint(5, 30)
        return rng.randint(30, 100)

    def sample_card_purchase_amount() -> float:
        tier = rng.choices(["MOSTLY", "SOMETIMES", "RARE"], weights=[85, 13, 2], k=1)[0]
        if tier == "MOSTLY":
            return -rng.uniform(5, 80)
        if tier == "SOMETIMES":
            return -rng.uniform(80, 300)
        return -rng.uniform(300, 1500)

    def sample_transfer_amount_and_direction() -> tuple[float, str]:
        dirn = rng.choices(["OUT", "IN"], weights=[55, 45], k=1)[0]
        if dirn == "OUT":
            return (-rng.uniform(50, 5000), "OUT")
        return (rng.uniform(200, 6000), "IN")

    def sample_atm_amount() -> float:
        return -rng.choice([20, 40, 50, 100, 200])

    def sample_bank_fee_amount() -> float:
        return -rng.uniform(1, 25)

    def sample_interest_amount() -> float:
        return rng.uniform(1, 100)

    def random_ts_in_month(month_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.Timestamp:
        total_seconds = int((month_end - month_start).total_seconds())
        if total_seconds <= 0:
            return month_start
        return month_start + pd.Timedelta(seconds=rng.randint(0, total_seconds))

    def make_value_ts(booking_ts: pd.Timestamp) -> pd.Timestamp:
        delta_days = rng.choices([0, 1, 2], weights=[60, 30, 10], k=1)[0]
        return booking_ts + pd.Timedelta(days=delta_days)

    def channel_for(txn_type: str) -> str:
        if txn_type == "CARD_PURCHASE":
            return "CARD"
        if txn_type == "TRANSFER":
            return "TRANSFER"
        if txn_type == "ATM_WITHDRAWAL":
            return "ATM"
        return "SYSTEM"

    def status_for(txn_type: str) -> str:
        r = rng.random()
        if txn_type == "CARD_PURCHASE":
            if r < 0.02:
                return "REVERSED"
            return "BOOKED"
        if txn_type == "TRANSFER":
            if r < 0.01:
                return "FAILED"
            return "BOOKED"
        return "BOOKED"

    # ---- generate ----
    rows = []
    txn_seq = 1

    for _, acc in a.iterrows():
        account_id = acc["account_id"]
        customer_id = acc["customer_id"]

        acc_start = max(acc["opened_at"], start_dt)
        acc_end = end_dt
        if pd.notna(acc["closed_at"]):
            acc_end = min(acc_end, acc["closed_at"])
        if acc_end < acc_start:
            continue

        month_cursor = pd.Timestamp(acc_start.year, acc_start.month, 1, tz="UTC")
        last_month = pd.Timestamp(acc_end.year, acc_end.month, 1, tz="UTC")

        while month_cursor <= last_month:
            month_start = month_cursor
            month_end = (month_cursor + pd.offsets.MonthBegin(1)) - pd.Timedelta(seconds=1)

            month_lo = max(month_start, acc_start)
            month_hi = min(month_end, acc_end)
            if month_hi < month_lo:
                month_cursor = (month_cursor + pd.offsets.MonthBegin(1))
                continue

            n_tx = sample_monthly_txn_count()

            for _ in range(n_tx):
                txn_type = rng.choices(txn_types, weights=txn_type_w, k=1)[0]

                booking_ts = random_ts_in_month(month_lo, month_hi)
                value_ts = make_value_ts(booking_ts)

                merchant_id = None
                counterparty_id = None

                if txn_type == "CARD_PURCHASE":
                    amount = sample_card_purchase_amount()
                    direction = "OUT"
                    merchant_id = pick_merchant_id()

                elif txn_type == "TRANSFER":
                    amount, direction = sample_transfer_amount_and_direction()
                    counterparty_id = pick_counterparty_id(customer_id, direction)

                elif txn_type == "ATM_WITHDRAWAL":
                    amount = sample_atm_amount()
                    direction = "OUT"
                    merchant_id = pick_merchant_id() if rng.random() < 0.30 else None

                elif txn_type == "BANK_FEE":
                    amount = sample_bank_fee_amount()
                    direction = "OUT"

                else:  # INTEREST
                    amount = sample_interest_amount()
                    direction = "IN"

                status = status_for(txn_type)
                channel = channel_for(txn_type)

                transaction_id = f"TXN{str(txn_seq).zfill(10)}"
                txn_seq += 1

                rows.append({
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "booking_ts": booking_ts.to_pydatetime().replace(microsecond=0).isoformat(),
                    "value_ts": value_ts.to_pydatetime().replace(microsecond=0).isoformat(),
                    "amount": round(float(amount), 2),
                    "currency": "EUR",
                    "direction": direction,
                    "channel": channel,
                    "merchant_id": merchant_id,
                    "counterparty_id": counterparty_id,
                    "txn_type": txn_type,
                    "status": status,
                })

            month_cursor = (month_cursor + pd.offsets.MonthBegin(1))

    return pd.DataFrame(rows)

# settlements generator

def generate_settlements(transactions_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:

    rng = random.Random(seed)

    t = transactions_df.copy()
    if t.empty:
        return pd.DataFrame(columns=[
            "settlement_id", "transaction_id", "settlement_date", "settled_amount",
            "currency", "fx_rate_used", "fees", "settlement_status"
        ])

    # Required columns
    required = {"transaction_id", "booking_ts", "amount", "currency", "txn_type", "status"}
    missing = required - set(t.columns)
    if missing:
        raise ValueError(f"transactions_df missing required columns: {sorted(missing)}")

    t["booking_ts"] = pd.to_datetime(t["booking_ts"], utc=True, errors="raise")

    def settlement_delay_days(txn_type: str) -> int:
        if txn_type == "CARD_PURCHASE":
            return rng.choices([1, 2, 3], weights=[50, 35, 15], k=1)[0]
        if txn_type == "TRANSFER":
            return rng.choices([0, 1, 2], weights=[25, 55, 20], k=1)[0]
        if txn_type == "ATM_WITHDRAWAL":
            return rng.choices([0, 1], weights=[70, 30], k=1)[0]
        # BANK_FEE / INTEREST: same day
        return 0

    def fee_amount(txn_type: str, amount: float) -> float:
        a = abs(amount)
        if txn_type == "CARD_PURCHASE":
            # small processing/interchange fee: ~0.1% to ~1%
            pct = rng.uniform(0.001, 0.010)
            fee = -min(a * pct, 8.00)  # cap fees to keep realistic
            return fee
        if txn_type == "ATM_WITHDRAWAL":
            # occasional ATM fee (some banks charge, some don't)
            if rng.random() < 0.35:
                return -rng.choice([1.50, 2.00, 2.50, 3.00])
            return 0.0
        if txn_type == "TRANSFER":
            # mostly free, sometimes small fee
            if rng.random() < 0.10:
                return -rng.choice([0.50, 1.00, 2.00, 3.00, 5.00])
            return 0.0
        # BANK_FEE / INTEREST: no additional settlement fee
        return 0.0

    rows = []
    stl_seq = 1

    for _, row in t.iterrows():
        txn_id = row["transaction_id"]
        txn_type = row["txn_type"]
        status = row["status"]
        amount = float(row["amount"])

        booking_ts = row["booking_ts"].to_pydatetime()
        delay = settlement_delay_days(txn_type)
        settlement_dt = booking_ts + timedelta(days=delay)

        # Settlement status + amounts
        if status == "FAILED":
            settlement_status = "FAILED"
            settled_amount = 0.0
            fees = 0.0
        elif status == "REVERSED":
            # reversed means it got cancelled; treat as not settled
            settlement_status = "REVERSED"
            settled_amount = 0.0
            fees = 0.0
        else:
            # BOOKED (or anything else treated as booked)
            # small chance to be pending
            settlement_status = "PENDING" if rng.random() < 0.02 else "SETTLED"
            settled_amount = amount
            fees = fee_amount(txn_type, amount)

        settlement_id = f"STL{str(stl_seq).zfill(10)}"
        stl_seq += 1

        rows.append({
            "settlement_id": settlement_id,
            "transaction_id": txn_id,
            "settlement_date": settlement_dt.date().isoformat(),
            "settled_amount": round(settled_amount, 2),
            "currency": "EUR",
            "fx_rate_used": 1.0,  # EUR-only scope
            "fees": round(float(fees), 2),
            "settlement_status": settlement_status
        })

    return pd.DataFrame(rows)

# disputes generator

def generate_disputes(transactions_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)

    t = transactions_df.copy()
    if t.empty:
        return pd.DataFrame(columns=[
            "dispute_id", "transaction_id", "reason",
            "opened_date", "resolved_date", "outcome"
        ])

    required = {"transaction_id", "booking_ts", "txn_type"}
    missing = required - set(t.columns)
    if missing:
        raise ValueError(f"transactions_df missing required columns: {sorted(missing)}")

    t["booking_ts"] = pd.to_datetime(t["booking_ts"], utc=True, errors="raise")

    dispute_rate = {
        "CARD_PURCHASE": 0.005,
        "TRANSFER": 0.001,
        "ATM_WITHDRAWAL": 0.0005,
        "BANK_FEE": 0.0,
        "INTEREST": 0.0,
    }

    reasons_by_type = {
        "CARD_PURCHASE": [
            "FRAUD_SUSPECTED",
            "DUPLICATE",
            "NOT_RECEIVED",
            "AMOUNT_INCORRECT",
            "MERCHANT_DISPUTE",
        ],
        "TRANSFER": [
            "UNAUTHORIZED_TRANSFER",
            "WRONG_BENEFICIARY",
            "SCAM_SUSPECTED",
        ],
        "ATM_WITHDRAWAL": [
            "ATM_CASH_NOT_RECEIVED",
            "ATM_PARTIAL_DISPENSE",
        ],
    }

    # outcomes (keep simple)
    outcomes = ["WON", "LOST", "PARTIAL", "PENDING"]
    outcome_w = [55, 30, 10, 5]

    def sample_opened_date(booking_ts: pd.Timestamp) -> pd.Timestamp:
        # dispute opened 0–60 days after booking
        return booking_ts + pd.Timedelta(days=rng.randint(0, 60))

    def sample_resolved_date(opened_ts: pd.Timestamp) -> str:
        # 10% unresolved -> None
        if rng.random() < 0.10:
            return None
        # resolved 5–90 days after opened
        resolved = opened_ts + pd.Timedelta(days=rng.randint(5, 90))
        return resolved.date().isoformat()

    rows = []
    dsp_seq = 1

    for _, row in t.iterrows():
        txn_type = row["txn_type"]
        rate = dispute_rate.get(txn_type, 0.0)

        if rate <= 0.0:
            continue
        if rng.random() >= rate:
            continue

        booking_ts = row["booking_ts"]
        opened_ts = sample_opened_date(booking_ts)

        reason = rng.choice(reasons_by_type.get(txn_type, ["OTHER"]))
        outcome = rng.choices(outcomes, weights=outcome_w, k=1)[0]

        dispute_id = f"DSP{str(dsp_seq).zfill(10)}"
        dsp_seq += 1

        rows.append({
            "dispute_id": dispute_id,
            "transaction_id": row["transaction_id"],
            "reason": reason,
            "opened_date": opened_ts.date().isoformat(),
            "resolved_date": sample_resolved_date(opened_ts),
            "outcome": outcome,
        })

    return pd.DataFrame(rows)