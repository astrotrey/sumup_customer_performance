/* =============================================== */
/* Create staging tables to load data directly     */
/* from the extracted CSVs                         */
/* =============================================== */
CREATE TABLE IF NOT EXISTS staging_devices (
    id integer,
    type integer,
    store_id integer
);

CREATE TABLE IF NOT EXISTS staging_stores (
    id integer,
    name text,
    address text,
    city text,
    country text,
    created_at timestamp,
    typology text,
    customer_id integer
);

CREATE TABLE IF NOT EXISTS staging_transactions (
    id integer,
    device_id integer,
    product_name1 text,
    product_sku text,
    product_name2 text,
    amount integer,
    status text,
    card_number text,
    cvv integer,
    created_at timestamp,
    happened_at timestamp
);

/* =============================================== */
/* Create persistent transactions data model       */
/* =============================================== */
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id integer PRIMARY KEY,
    device_id integer,
    store_id integer,
    customer_id integer,
    device_type integer,
    store_name text, 
    store_type text, 
    store_country text,
    store_created_at timestamp,
    transaction_created_at timestamp,
    transaction_happened_at timestamp,
    transaction_amount integer,
    transaction_status text,
    card_number text,
    cvv integer,
    product_sku text,
    product_name1 text,
    product_name2 text
);

/* =============================================== */
/* Create persistent transaction_rate              */
/* data model. The other metrics do not require a  */
/* persistent table because their queries should   */
/* still execute quickly even at large scales      */
/* =============================================== */
CREATE TABLE IF NOT EXISTS transaction_rate (
    customer_id integer,
    store_id integer PRIMARY KEY,
    first_transaction_at timestamp,
    fifth_transaction_at timestamp,
    days_to_5_transactions integer
);
