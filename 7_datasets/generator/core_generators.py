import random
import string
from datetime import datetime, timedelta, date, timezone
import pandas as pd
from faker import Faker

# customers generator

def generate_customers(n_customers: int = 5000, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)
    faker = Faker(["fr_FR", "de_DE", "en_GB"])
    Faker.seed(seed)

    countries = ["Luxembourg", "France", "Germany", "Belgium", "Switzerland"]
    country_weights = [60, 15, 10, 10, 5]

    now = datetime.utcnow()
    created_start = now - timedelta(days=5 * 365)  # last ~5 years
    created_end = now - timedelta(days=30)         # avoid "created today"

    rows = []
    for i in range(1, n_customers + 1):
        customer_id = f"CUST{str(i).zfill(6)}"

        first_name = faker.first_name()
        last_name = faker.last_name()

        age_years = rng.choices(
            population=[18, 25, 35, 45, 55, 65, 75, 85],
            weights=[10, 18, 20, 18, 14, 10, 6, 4],
            k=1
        )[0]
        jitter = rng.randint(-3, 3)
        age_years = max(18, min(90, age_years + jitter))

        dob_year = now.year - age_years
        dob = date(dob_year, rng.randint(1, 12), rng.randint(1, 28)).isoformat()

        country = rng.choices(countries, weights=country_weights, k=1)[0]

        created_at = created_start + timedelta(seconds=rng.randint(0, int((created_end - created_start).total_seconds())))

        # 70% never updated, 30% updated after creation
        if rng.random() < 0.7:
            updated_at = created_at
        else:
            updated_at = created_at + timedelta(seconds=rng.randint(0, int((now - created_at).total_seconds())))

        rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob,
            "country": country,
            "created_at": created_at.isoformat(timespec="seconds"),
            "updated_at": updated_at.isoformat(timespec="seconds"),
        })

    return pd.DataFrame(rows)

# accounts generator

def generate_accounts(customers_df: pd.DataFrame, products_df: pd.DataFrame, branches_df: pd.DataFrame,
                    seed: int = 42) -> pd.DataFrame:
    
    rng = random.Random(seed)

    # Ensure datetime types
    c = customers_df.copy()
    c["created_at"] = pd.to_datetime(c["created_at"], utc=True, errors="raise")
    c["updated_at"] = pd.to_datetime(c["updated_at"], utc=True, errors="raise")

    product_ids = products_df["product_id"].tolist()
    branch_ids = branches_df["branch_id"].tolist()

    acct_count_values = [1, 2, 3, 4]
    acct_count_weights = [80, 15, 4, 1]

    def make_iban() -> str:
        # Generate a (not valid) LU-style IBAN-like string.
        # LU IBAN length is 20 chars: 'LU' + 2 check digits + 16 alphanumerics.
        check_digits = f"{rng.randint(0, 99):02d}"
        body = "".join(rng.choice(string.ascii_uppercase + string.digits) for _ in range(16))
        return f"LU{check_digits}{body}"

    rows = []
    acc_seq = 1

    now_utc = datetime.now(timezone.utc)

    for _, row in c.iterrows():
        customer_id = row["customer_id"]
        cust_created = row["created_at"].to_pydatetime()
        cust_updated = row["updated_at"].to_pydatetime()

        upper = min(cust_updated, now_utc)
        lower = cust_created if cust_created <= upper else upper

        n_accounts = rng.choices(acct_count_values, weights=acct_count_weights, k=1)[0]

        for _ in range(n_accounts):
            account_id = f"ACC{str(acc_seq).zfill(7)}"
            acc_seq += 1

            product_id = rng.choice(product_ids)
            branch_id = rng.choice(branch_ids)

            # opened_at between customer.created_at and upper bound
            window_seconds = int((upper - lower).total_seconds())
            opened_at = lower + timedelta(seconds=rng.randint(0, max(window_seconds, 0)))

            # Decide if closed: small probability; if closed, closed_at >= opened_at
            is_closed = rng.random() < 0.12  # ~12% closed accounts
            if is_closed:
                close_window_seconds = int((upper - opened_at).total_seconds())
                closed_at = opened_at + timedelta(seconds=rng.randint(0, max(close_window_seconds, 0)))
                status = "CLOSED"
                closed_at_str = closed_at.replace(microsecond=0).isoformat()
            else:
                status = "OPEN"
                closed_at_str = None

            rows.append({
                "account_id": account_id,
                "customer_id": customer_id,
                "iban": make_iban(),
                "product_id": product_id,
                "branch_id": branch_id,
                "currency": "EUR",
                "status": status,
                "opened_at": opened_at.replace(microsecond=0).isoformat(),
                "closed_at": closed_at_str
            })

    return pd.DataFrame(rows)
