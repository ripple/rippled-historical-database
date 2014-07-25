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
    DROP CONSTRAINT IF EXISTS fk_from_account;

-----------------------------------------------------------------------------

DROP TABLE IF EXISTS ledgers;
DROP TABLE IF EXISTS account_transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS ledger_transactions;

-----------------------------------------------------------------------------

CREATE TABLE ledgers (
    id                    BIGSERIAL PRIMARY KEY,
    hash                  bytea,
    sequence              BIGINT,
    prev_hash             bytea,
    total_coins           BIGINT,
    closing_time          BIGINT,
    prev_closing_time     BIGINT,
    close_time_resolution BIGINT,
    close_flags           BIGINT,
    account_set_hash      bytea,
    transaction_set_hash  bytea
);

CREATE UNIQUE INDEX ledger_sequence_index
          ON ledgers(sequence);

CREATE UNIQUE INDEX ledger_hash_index
          ON ledgers(hash);

CREATE INDEX ledger_time_index
          ON ledgers(closing_time);

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
    id              BIGSERIAL PRIMARY KEY,
    hash            bytea,
    type            transaction_type,
    from_account    BIGINT,
    from_sequence   BIGINT,
    ledger_sequence BIGINT,
    status          CHAR(1),
    raw             bytea,
    meta            bytea
);

CREATE INDEX transaction_ledger_index
          ON transactions(ledger_sequence);

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
   ADD CONSTRAINT fk_from_account FOREIGN KEY(from_account)
                                  REFERENCES accounts(id);

-----------------------------------------------------------------------------
-- vim: set syntax=sql:
