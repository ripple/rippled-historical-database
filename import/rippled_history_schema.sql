--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: transaction_type; Type: TYPE; Schema: public; Owner: rippled
--

CREATE TYPE transaction_type AS ENUM (
    'Payment',
    'OfferCreate',
    'OfferCancel',
    'AccountSet',
    'SetRegularKey',
    'TrustSet',
    'EnableAmendment',
    'SetFee'
);


ALTER TYPE public.transaction_type OWNER TO rippled;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: account_transactions; Type: TABLE; Schema: public; Owner: rippled; Tablespace: 
--

CREATE TABLE account_transactions (
    transaction_id bigint NOT NULL,
    account_id bigint NOT NULL,
    ledger_sequence bigint NOT NULL,
    transaction_sequence bigint NOT NULL
);


ALTER TABLE public.account_transactions OWNER TO rippled;

--
-- Name: accounts; Type: TABLE; Schema: public; Owner: rippled; Tablespace: 
--

CREATE TABLE accounts (
    id bigint NOT NULL,
    address bytea NOT NULL
);


ALTER TABLE public.accounts OWNER TO rippled;

--
-- Name: accounts_id_seq; Type: SEQUENCE; Schema: public; Owner: rippled
--

CREATE SEQUENCE accounts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.accounts_id_seq OWNER TO rippled;

--
-- Name: accounts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: rippled
--

ALTER SEQUENCE accounts_id_seq OWNED BY accounts.id;


--
-- Name: ledger_transactions; Type: TABLE; Schema: public; Owner: rippled; Tablespace: 
--

CREATE TABLE ledger_transactions (
    transaction_id bigint NOT NULL,
    ledger_id bigint NOT NULL,
    transaction_sequence bigint NOT NULL
);


ALTER TABLE public.ledger_transactions OWNER TO rippled;

--
-- Name: ledgers; Type: TABLE; Schema: public; Owner: rippled; Tablespace: 
--

CREATE TABLE ledgers (
    id bigint NOT NULL,
    ledger_hash bytea NOT NULL,
    parent_hash bytea,
    total_coins bigint,
    close_time bigint,
    close_time_resolution bigint,
    account_hash bytea,
    transaction_hash bytea,
    accepted boolean,
    closed boolean,
    close_time_estimated boolean,
    close_time_human timestamp with time zone
);


ALTER TABLE public.ledgers OWNER TO rippled;

--
-- Name: ledgers_id_seq; Type: SEQUENCE; Schema: public; Owner: rippled
--

CREATE SEQUENCE ledgers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ledgers_id_seq OWNER TO rippled;

--
-- Name: ledgers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: rippled
--

ALTER SEQUENCE ledgers_id_seq OWNED BY ledgers.id;


--
-- Name: transactions; Type: TABLE; Schema: public; Owner: rippled; Tablespace: 
--

CREATE TABLE transactions (
    id bigint NOT NULL,
    account bytea,
    destination bytea,
    fee bigint,
    flags bigint,
    paths bytea,
    send_max bytea,
    offer_sequence bigint,
    sequence bigint,
    signing_pub_key bytea,
    taker_gets bytea,
    taker_pays bytea,
    transaction_type transaction_type,
    txn_signature bytea,
    hash bytea NOT NULL,
    meta_data bytea
);


ALTER TABLE public.transactions OWNER TO rippled;

--
-- Name: transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: rippled
--

CREATE SEQUENCE transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.transactions_id_seq OWNER TO rippled;

--
-- Name: transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: rippled
--

ALTER SEQUENCE transactions_id_seq OWNED BY transactions.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: rippled
--

ALTER TABLE ONLY accounts ALTER COLUMN id SET DEFAULT nextval('accounts_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: rippled
--

ALTER TABLE ONLY ledgers ALTER COLUMN id SET DEFAULT nextval('ledgers_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: rippled
--

ALTER TABLE ONLY transactions ALTER COLUMN id SET DEFAULT nextval('transactions_id_seq'::regclass);


--
-- Name: accounts_address_key; Type: CONSTRAINT; Schema: public; Owner: rippled; Tablespace: 
--

ALTER TABLE ONLY accounts
    ADD CONSTRAINT accounts_address_key UNIQUE (address);


--
-- Name: accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: rippled; Tablespace: 
--

ALTER TABLE ONLY accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);


--
-- Name: ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: rippled; Tablespace: 
--

ALTER TABLE ONLY ledgers
    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (id);


--
-- Name: transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: rippled; Tablespace: 
--

ALTER TABLE ONLY transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (id);


--
-- Name: account_ledger_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX account_ledger_index ON account_transactions USING btree (ledger_sequence, account_id, transaction_id);


--
-- Name: account_transaction_id_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX account_transaction_id_index ON account_transactions USING btree (transaction_id);


--
-- Name: account_transaction_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX account_transaction_index ON account_transactions USING btree (account_id, ledger_sequence, transaction_sequence);


--
-- Name: accounts_address_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX accounts_address_index ON accounts USING btree (address);


--
-- Name: ledger_close_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX ledger_close_index ON ledgers USING btree (close_time);


--
-- Name: ledger_hash_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX ledger_hash_index ON ledgers USING btree (ledger_hash);


--
-- Name: ledgers_ledger_hash_idx; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE UNIQUE INDEX ledgers_ledger_hash_idx ON ledgers USING btree (ledger_hash);


--
-- Name: transaction_ledger_index; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE INDEX transaction_ledger_index ON transactions USING btree (sequence);


--
-- Name: transactions_hash_idx; Type: INDEX; Schema: public; Owner: rippled; Tablespace: 
--

CREATE UNIQUE INDEX transactions_hash_idx ON transactions USING btree (hash);


--
-- Name: fk_account_id; Type: FK CONSTRAINT; Schema: public; Owner: rippled
--

ALTER TABLE ONLY account_transactions
    ADD CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES accounts(id);


--
-- Name: fk_transaction_id; Type: FK CONSTRAINT; Schema: public; Owner: rippled
--

ALTER TABLE ONLY account_transactions
    ADD CONSTRAINT fk_transaction_id FOREIGN KEY (transaction_id) REFERENCES transactions(id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

