-- For database tlm-bank-a --

-- Re-runnable drops
DROP TABLE IF EXISTS dbo.transactions;
DROP TABLE IF EXISTS dbo.accounts;
DROP TABLE IF EXISTS dbo.merchants;
DROP TABLE IF EXISTS dbo.counterparties;
DROP TABLE IF EXISTS dbo.products;
DROP TABLE IF EXISTS dbo.branches;
DROP TABLE IF EXISTS dbo.customers;

-- 1. customers table

CREATE TABLE dbo.customers (
    customer_id NVARCHAR(50) NOT NULL,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    dob date NOT NULL,
    country NVARCHAR(50) NOT NULL,
    created_at DATETIME2 NOT NULL,
    updated_at DATETIME2,
    CONSTRAINT PK_customers PRIMARY KEY (customer_id)
);

-- 2. branches table

CREATE TABLE dbo.branches (
    branch_id NVARCHAR(50) NOT NULL,
    branch_name NVARCHAR(50) NOT NULL,
    city NVARCHAR(50) NOT NULL,
    country NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_branches PRIMARY KEY (branch_id)
);

-- 3. products table

CREATE TABLE dbo.products (
    product_id NVARCHAR(50) NOT NULL,
    product_type NVARCHAR(50) NOT NULL,
    currency NVARCHAR(50) NOT NULL,
    interest_rate DECIMAL(9,4) NOT NULL,
    monthly_fee DECIMAL(18,2) NOT NULL,
    CONSTRAINT PK_products PRIMARY KEY (product_id)
);

-- 4. accounts table

CREATE TABLE dbo.accounts (
    account_id NVARCHAR(50) NOT NULL,
    customer_id NVARCHAR(50) NOT NULL,
    iban NVARCHAR(50) NOT NULL,
    product_id NVARCHAR(50) NOT NULL,
    branch_id NVARCHAR(50) NOT NULL,
    currency NVARCHAR(50) NOT NULL,
    status NVARCHAR(50) NOT NULL,
    opened_at DATETIME2 NOT NULL,
    closed_at DATETIME2,
    CONSTRAINT PK_accounts PRIMARY KEY (account_id),
    CONSTRAINT FK_accounts_customers FOREIGN KEY (customer_id)
        REFERENCES dbo.customers (customer_id),
    CONSTRAINT FK_accounts_products FOREIGN KEY (product_id)
        REFERENCES dbo.products (product_id),
    CONSTRAINT FK_accounts_branches FOREIGN KEY (branch_id)
        REFERENCES dbo.branches (branch_id)
);

-- 5. merchants table

CREATE TABLE dbo.merchants (
    merchant_id NVARCHAR(50) NOT NULL,
    merchant_name NVARCHAR(50) NOT NULL,
    mcc_code NVARCHAR(50) NOT NULL,
    country NVARCHAR(50) NOT NULL,
    city NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_merchants PRIMARY KEY (merchant_id)
);

-- 6. counterparties table

CREATE TABLE dbo.counterparties (
    counterparty_id NVARCHAR(50) NOT NULL,
    counterparty_name NVARCHAR(50) NOT NULL,
    counterparty_type NVARCHAR(50) NOT NULL,
    bank_bic NVARCHAR(50) NULL,
    country NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_counterparties PRIMARY KEY (counterparty_id)
);


-- 7. transactions table

CREATE TABLE dbo.transactions (
    transaction_id NVARCHAR(50) NOT NULL,
    account_id NVARCHAR(50) NOT NULL,
    booking_ts DATETIME2 NOT NULL,
    value_ts DATETIME2 NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    currency NVARCHAR(50) NOT NULL,
    direction NVARCHAR(50) NOT NULL,
    channel NVARCHAR(50) NOT NULL,
    merchant_id NVARCHAR(50) NULL,
    counterparty_id NVARCHAR(50) NULL,
    txn_type NVARCHAR(50) NOT NULL,
    status NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT FK_transactions_accounts FOREIGN KEY (account_id)
        REFERENCES dbo.accounts (account_id),
    CONSTRAINT FK_transactions_merchants FOREIGN KEY (merchant_id)
        REFERENCES dbo.merchants (merchant_id),
    CONSTRAINT FK_transactions_counterparties FOREIGN KEY (counterparty_id)
        REFERENCES dbo.counterparties (counterparty_id)
);
