from core_generators import generate_customers, generate_accounts
from reference_generators import (
                                    generate_products,
                                    generate_branches_banka,
                                    generate_branches_bankb,
                                    generate_merchants,
                                    generate_counterparties,
                                    generate_mcc)
from fact_generators import generate_transactions, generate_settlements, generate_disputes


def main() -> None:
# Bank A
    customersA_df = generate_customers(seed = 42)
    customersA_df.to_csv(r"../data/bank_a/customers.csv", index=False)

    branchesA_df = generate_branches_banka()
    branchesA_df.to_csv(r"../data/bank_a/branches.csv", index=False)

    productsA_df = generate_products()
    productsA_df.to_csv(r"../data/bank_a/products.csv", index=False)

    accountsA_df = generate_accounts(customersA_df, productsA_df, branchesA_df, seed = 42)
    accountsA_df.to_csv(r"../data/bank_a/accounts.csv", index=False)

    merchantsA_df = generate_merchants(branchesA_df, seed = 42)
    merchantsA_df.to_csv(r"../data/bank_a/merchants.csv", index=False)

    counterpartiesA_df = generate_counterparties(seed = 42)
    counterpartiesA_df.to_csv(r"../data/bank_a/counterparties.csv", index=False)

    transactionsA_df = generate_transactions(accountsA_df, merchantsA_df, counterpartiesA_df, seed = 42)
    transactionsA_df.to_csv(r"../data/bank_a/transactions.csv", index=False)

# Bank B
    customersB_df = generate_customers(seed = 4242)
    customersB_df.to_csv(r"../data/bank_b/customers.csv", index=False)

    branchesB_df = generate_branches_bankb()
    branchesB_df.to_csv(r"../data/bank_b/branches.csv", index=False)

    productsB_df = generate_products()
    productsB_df.to_csv(r"../data/bank_b/products.csv", index=False)

    accountsB_df = generate_accounts(customersB_df, productsB_df, branchesB_df, seed = 4242)
    accountsB_df.to_csv(r"../data/bank_b/accounts.csv", index=False)

    merchantsB_df = generate_merchants(branchesB_df, seed = 4242)
    merchantsB_df.to_csv(r"../data/bank_b/merchants.csv", index=False)

    counterpartiesB_df = generate_counterparties(seed = 4242)
    counterpartiesB_df.to_csv(r"../data/bank_b/counterparties.csv", index=False)

    transactionsB_df = generate_transactions(accountsB_df, merchantsB_df, counterpartiesB_df, seed = 4242)
    transactionsB_df.to_csv(r"../data/bank_b/transactions.csv", index=False)

# Flat Files
    mcc_df = generate_mcc()
    mcc_df.to_csv(r"../data/flat_files/mcc_codes.csv", index=False)

    settlementsA_df = generate_settlements(transactionsA_df, seed = 42)
    settlementsA_df.to_csv(r"../data/flat_files/bank_a_settlements.csv", index=False)

    settlementsB_df = generate_settlements(transactionsB_df, seed = 4242)
    settlementsB_df.to_csv(r"../data/flat_files/bank_b_settlements.csv", index=False)

    disputesA_df = generate_disputes(transactionsA_df, seed = 42)
    disputesA_df.to_csv(r"../data/flat_files/bank_a_disputes.csv", index=False)

    disputesB_df = generate_disputes(transactionsB_df, seed = 4242)
    disputesB_df.to_csv(r"../data/flat_files/bank_b_disputes.csv", index=False)

if __name__ == "__main__":
    main()