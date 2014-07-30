-- schema.sql
--
-- This file contains the Postgres commands needed to recreate the Ripple
-- History project's database schema.  It automatically deletes any existing
-- tables and related objects before recreating them, ensuring that the
-- database is restored back to a pristine state each time it is run.

-----------------------------------------------------------------------------

ALTER TABLE IF EXISTS account_transactions
    DROP CONSTRAINT IF EXISTS fk_transaction_id,
    DROP CONSTRAINT IF EXISTS fk_account_id;

ALTER TABLE IF EXISTS transactions
    DROP CONSTRAINT IF EXISTS fk_account_id;

-----------------------------------------------------------------------------

DROP TABLE IF EXISTS ledgers;
DROP TABLE IF EXISTS account_transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS ledger_transactions;

-----------------------------------------------------------------------------

-- NOTES:
-- The parent_closing_time is not in the json response but
--   is in the rippled header
-- Where are the close_flags?

CREATE TABLE ledgers (
    id                    BIGSERIAL PRIMARY KEY,
    ledger_hash           bytea,
    parent_hash           bytea,
    total_coins           BIGINT,
    close_time            BIGINT,
    close_time_resolution BIGINT,
    account_hash          bytea,
    transaction_hash      bytea,
    accepted              BOOLEAN,
    closed                BOOLEAN,
    close_time_estimated  BOOLEAN,
    close_time_human      TIMESTAMP WITH TIME ZONE

    -- NOTE: Not in the JSON response:
    -- parent_close_time     BIGINT,
    -- close_flags           BIGINT,
    -- state_hash            bytea
);

CREATE INDEX ledger_hash_index
          ON ledgers(ledger_hash);

CREATE INDEX ledger_close_index
          ON ledgers(close_time);

-----------------------------------------------------------------------------

CREATE TABLE account_transactions (
    transaction_id       BIGINT,
    account_id           BIGINT,
    ledger_sequence      BIGINT,
    transaction_sequence BIGINT
);

CREATE INDEX account_transaction_id_index
          ON account_transactions(transaction_id);

CREATE INDEX account_transaction_index
          ON account_transactions(account_id, ledger_sequence,
                                  transaction_sequence);

CREATE INDEX account_ledger_index
          ON account_transactions(ledger_sequence, account_id, transaction_id);

-----------------------------------------------------------------------------

CREATE TABLE accounts (
    id         BIGSERIAL PRIMARY KEY,
    address    bytea
);

-----------------------------------------------------------------------------

DROP TYPE IF EXISTS transaction_type;
CREATE TYPE transaction_type AS ENUM('Payment', 'OfferCreate', 'OfferCancel',
                                     'AccountSet', 'SetRegularKey',
                                     'TrustSet');

-----------------------------------------------------------------------------

CREATE TABLE transactions (
    id               BIGSERIAL PRIMARY KEY,
    account          bytea,
    flags            BIGINT,
    offer_sequence   BIGINT,
    sequence         BIGINT,
    signing_pub_key  bytea,
    transaction_type transaction_type,
    txn_signature    bytea,
    hash             bytea,
    meta_data        bytea
);

CREATE INDEX transaction_ledger_index
          ON transactions(sequence);

-----------------------------------------------------------------------------

CREATE TABLE ledger_transactions (
    transaction_id       BIGINT,
    ledger_id            BIGINT,
    transaction_sequence BIGINT
);

-----------------------------------------------------------------------------

ALTER TABLE account_transactions
    ADD CONSTRAINT fk_transaction_id FOREIGN KEY (transaction_id)
                                     REFERENCES transactions(id),
    ADD CONSTRAINT fk_account_id FOREIGN KEY (account_id)
                                 REFERENCES accounts(id);

-----------------------------------------------------------------------------

ALTER TABLE transactions
   ADD CONSTRAINT fk_account_id FOREIGN KEY(id)
                                REFERENCES accounts(id);

-----------------------------------------------------------------------------
-- vim: set syntax=sql:
