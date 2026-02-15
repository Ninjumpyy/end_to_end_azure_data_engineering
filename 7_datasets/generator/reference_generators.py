import pandas as pd
import random
from faker import Faker

# MCC-codes dataset

def generate_mcc() -> pd.DataFrame:
    mcc = pd.read_csv(r'../data/raw/mcc_codes_source.csv',
                      dtype={"mcc": str})
    mcc = mcc[["mcc", "edited_description"]]
    mcc.rename(columns={"mcc": "mcc_code", "edited_description": "mcc_description"}, inplace=True)
    mcc["mcc_code"] = mcc["mcc_code"].str.zfill(4)

    # MCC category ranges
    bins = [0,1499,2999,3299,3499,3999,4799,4999,5599,5699,7299,7999,8999,9999]
    labels = ["Agricultural Services", "Contracted Services", "Airlines", "Car Rental", "Lodging", "Transportation Services", 
              "Utility Services", "Retail Outlet Services", "Clothing Stores", "Miscellaneous Stores", "Business Services",
              "Professional Services and Membership Organizations", "Government Services"]
    mcc["mcc_category"] = pd.cut(
        mcc["mcc_code"].astype(int),
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True
    )
    return (mcc)

# Branches table

def generate_branches_banka() -> pd.DataFrame:
    num_branches = 10
    branches = {
        "branch_id": [f"BR{str(i).zfill(3)}" for i in range(1, num_branches + 1)],
        "branch_name": [
        "BankA Luxembourg", "BankA Esch-sur-Alzette", "BankA Differdange",
        "BankA Dudelange", "BankA Trier", "BankA Saarbrucken", "BankA Metz",
        "BankA Thionville", "BankA Arlon", "BankA Basel"],
        "city": [
            "Luxembourg", "Esch-sur-Alzette", "Differdange", "Dudelange", "Trier", "Saarbrucken",
            "Metz", "Thionville", "Arlon", "Basel"],
        "country": [
            "Luxembourg", "Luxembourg", "Luxembourg", "Luxembourg", "Germany", "Germany",
            "France", "France", "Belgium", "Switzerland"]
    }
    return(pd.DataFrame(branches))

def generate_branches_bankb() -> pd.DataFrame:
    num_branches = 10
    branches = {
        "branch_id": [f"BR{str(i).zfill(3)}" for i in range(1, num_branches + 1)],
        "branch_name": [
        "BankB Luxembourg" ,"BankB Ettelbruck", "BankB Diekirch", 
        "BankB Wiltz","BankB Nancy", "BankB Metz", "BankB Trier",
        "BankB Brussels", "BankB Liege","BankB Basel"],
        "city": [
            "Luxembourg", "Ettelbruck", "Diekirch", "Wiltz", "Nancy", "Metz", "Trier",
            "Brussels", "Liege", "Basel"],
        "country": [
            "Luxembourg", "Luxembourg", "Luxembourg", "Luxembourg", "France", "France",
            "Germany", "Belgium", "Belgium", "Switzerland"]
    }
    return(pd.DataFrame(branches))

# products dataset

def generate_products() -> pd.DataFrame:
    num_products = 8
    products = {
        "product_id": [f"PR{str(i).zfill(3)}" for i in range(1, num_products + 1)],
        "product_type": ["CHECKING_BASIC", "CHECKING_PREMIUM", "SAVINGS_STANDARD", "SAVINGS_HIGH_YIELD",
                        "CREDIT_STANDARD", "CREDIT_PREMIUM", "LOAN_PERSONAL", "LOAN_STUDENT"],
        "currency": ["EUR"] * num_products,
        "interest_rate": [0.0, 0.0, 1.5, 3.0, 15.0, 12.0, 6.0, 2.0],
        "monthly_fee": [0.0, 10.0, 0.0, 0.0, 0.0, 5.0, 0.0, 0.0]
    }
    return(pd.DataFrame(products))

# merchants dataset

def generate_merchants(branches_df: pd.DataFrame, n_merchants: int = 200, seed: int = 42) -> pd.DataFrame:
    random.seed(seed)

    city_country = (
        branches_df[["city", "country"]]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    mcc_cfg = [
        {"mcc_code": "5411", "weight": 20, "global": False, "templates": [
            "FreshMart {City}", "{City} Supermarket", "GreenFood {City}", "MarketPlus {City}"
        ]},
        {"mcc_code": "5812", "weight": 15, "global": False, "templates": [
            "{City} Bistro", "La Table {City}", "{Cuisine} Kitchen {City}", "{City} Restaurant"
        ]},
        {"mcc_code": "5814", "weight": 10, "global": False, "templates": [
            "QuickBite {City}", "Burger Point {City}", "FastEat {City}", "{City} Express"
        ]},
        {"mcc_code": "5542", "weight": 10, "global": False, "templates": [
            "EuroFuel {City}", "PetroLux {City}", "AutoFuel {City}", "{City} Fuel Station"
        ]},
        {"mcc_code": "5311", "weight": 8, "global": False, "templates": [
            "CityMall {City}", "Grand Store {City}", "{City} Department Store"
        ]},
        {"mcc_code": "5732", "weight": 6, "global": False, "templates": [
            "TechStore {City}", "Digital World {City}", "ElectroHub {City}"
        ]},
        {"mcc_code": "5651", "weight": 6, "global": False, "templates": [
            "StyleHouse {City}", "{City} Fashion", "UrbanWear {City}"
        ]},
        {"mcc_code": "5912", "weight": 6, "global": False, "templates": [
            "{City} Pharmacy", "HealthPlus {City}", "MediCare {City}"
        ]},
        {"mcc_code": "4111", "weight": 5, "global": False, "templates": [
            "{City} Mobility", "{City} Transport Services", "{Country} Rail"
        ]},
        {"mcc_code": "7011", "weight": 4, "global": False, "templates": [
            "Hotel {City} Central", "Grand Hotel {City}", "{City} Inn"
        ]},
        {"mcc_code": "4511", "weight": 3, "global": True, "templates": [
            "EuroAir", "Lux Airways", "Central Europe Airlines"
        ]},
        {"mcc_code": "7399", "weight": 3, "global": True, "templates": [
            "CloudServices EU", "Business Services Europe", "ProServices Ltd"
        ]},
        {"mcc_code": "8299", "weight": 3, "global": False, "templates": [
            "{City} Training Institute", "{Country} Education Services", "{City} Learning Center"
        ]},
        {"mcc_code": "6011", "weight": 1, "global": False, "templates": [
            "ATM {City}", "{BankName} ATM {City}"
        ]},
        {"mcc_code": "5967", "weight": 1, "global": True, "templates": [
            "Digital Media Plus", "Online Services Ltd", "Subscription Hub"
        ]},
    ]

    cuisines = ["Italian", "French", "Asian", "Lebanese", "Indian", "Japanese", "Mexican"]
    bank_names = ["BankA", "BankB"]

    total_w = sum(x["weight"] for x in mcc_cfg)
    counts = [int(round(n_merchants * x["weight"] / total_w)) for x in mcc_cfg]
    drift = n_merchants - sum(counts)

    order = sorted(range(len(mcc_cfg)), key=lambda i: mcc_cfg[i]["weight"], reverse=True)
    idx = 0
    while drift != 0:
        i = order[idx % len(order)] # Walk through the priority list again and again until the total count matches exactly.
        if drift > 0:
            counts[i] += 1
            drift -= 1
        else:
            if counts[i] > 0:
                counts[i] -= 1
                drift += 1
        idx += 1

    merchants = []
    mid = 1

    for cfg, k in zip(mcc_cfg, counts):
        for _ in range(k):
            merchant_id = f"MRC{str(mid).zfill(6)}"
            mid += 1

            if cfg["global"]:
                city = "ONLINE"
                country = "MULTI"
            else:
                loc = random.choice(city_country)
                city = loc["city"]
                country = loc["country"]

            template = random.choice(cfg["templates"])
            merchant_name = template.format(
                City=city,
                Country=country,
                Cuisine=random.choice(cuisines),
                BankName=random.choice(bank_names)
            )

            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "mcc_code": cfg["mcc_code"],
                "country": country,
                "city": city
            })

    return pd.DataFrame(merchants)

# Counterparties dataset

def generate_counterparties(n_counterparties: int = 400, seed: int = 42) -> pd.DataFrame:
    random.seed(seed)
    Faker.seed(seed)
    faker = Faker(["fr_FR", "de_DE", "en_GB"])

    type_cfg = {
        "PERSONAL": 45,
        "EMPLOYER": 20,
        "LANDLORD": 15,  
        "BUSINESS": 15, 
        "FINANCIAL_INSTITUTION": 5
    }

    countries = ["Luxembourg", "France", "Germany", "Belgium", "Switzerland"]

    employer_names = [
        "LuxTech SA", "EuroFinance Group", "Central IT Solutions",
        "Global Consulting SARL", "Nordic Systems", "Alpine Industries"
    ]
    landlord_names = [
        "Urban Living SARL", "RealEstate Holdings SA",
        "City Property Management", "GreenHomes Group"
    ]
    business_names = [
        "Consulting Plus", "IT Services Europe",
        "Maintenance Pro", "Office Supplies Co", "Logistics Partner"
    ]
    financial_institutions = [
        "BNP Paribas", "Deutsche Bank", "ING Belgium",
        "Credit Suisse", "Banque Internationale Luxembourg"
    ]

    def fake_bic(country: str) -> str:
        # 8-char plausible BIC: 4 letters bank + 2 country + 2 letters
        return (faker.lexify(text="????").upper() + country[:2].upper() + faker.lexify(text="??").upper())

    total_w = sum(type_cfg.values())
    counts = {
        t: int(round(n_counterparties * w / total_w)) for t, w in type_cfg.items()
    }
    drift = n_counterparties - sum(counts.values())
    for t in sorted(type_cfg, key=type_cfg.get, reverse=True):
        if drift == 0:
            break
        counts[t] += 1
        drift -= 1

    rows = []
    cid = 1

    for ctype, k in counts.items():
        for _ in range(k):
            counterparty_id = f"CP{str(cid).zfill(6)}"
            cid += 1

            country = random.choice(countries)

            if ctype == "PERSONAL":
                name = faker.name()
                bic = None

            elif ctype == "EMPLOYER":
                name = random.choice(employer_names)
                bic = fake_bic(country)

            elif ctype == "LANDLORD":
                name = random.choice(landlord_names)
                bic = fake_bic(country)

            elif ctype == "BUSINESS":
                name = random.choice(business_names)
                bic = fake_bic(country)

            elif ctype == "FINANCIAL_INSTITUTION":
                name = random.choice(financial_institutions)
                bic = fake_bic(country)

            rows.append({
                "counterparty_id": counterparty_id,
                "counterparty_name": name,
                "counterparty_type": ctype,
                "bank_bic": bic,
                "country": country
            })

    return pd.DataFrame(rows)
