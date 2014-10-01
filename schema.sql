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

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: account_transactions; Type: TABLE; Schema: public; Owner: matthew; Tablespace: 
--

CREATE TABLE account_transactions (
    account_id bigint,
    tx_id bigint
);


ALTER TABLE public.account_transactions OWNER TO matthew;

--
-- Name: accounts; Type: TABLE; Schema: public; Owner: matthew; Tablespace: 
--

CREATE TABLE accounts (
    account_id bigint NOT NULL,
    account character varying(64),
    parent character varying(64),
    tx_hash bytea,
    created_time bigint
);


ALTER TABLE public.accounts OWNER TO matthew;

--
-- Name: accounts_account_id_seq; Type: SEQUENCE; Schema: public; Owner: matthew
--

CREATE SEQUENCE accounts_account_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.accounts_account_id_seq OWNER TO matthew;

--
-- Name: accounts_account_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: matthew
--

ALTER SEQUENCE accounts_account_id_seq OWNED BY accounts.account_id;


--
-- Name: knex_migrations; Type: TABLE; Schema: public; Owner: matthew; Tablespace: 
--

CREATE TABLE knex_migrations (
    id integer NOT NULL,
    name character varying(255),
    batch integer,
    migration_time timestamp with time zone
);


ALTER TABLE public.knex_migrations OWNER TO matthew;

--
-- Name: knex_migrations_id_seq; Type: SEQUENCE; Schema: public; Owner: matthew
--

CREATE SEQUENCE knex_migrations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.knex_migrations_id_seq OWNER TO matthew;

--
-- Name: knex_migrations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: matthew
--

ALTER SEQUENCE knex_migrations_id_seq OWNED BY knex_migrations.id;


--
-- Name: ledgers; Type: TABLE; Schema: public; Owner: matthew; Tablespace: 
--

CREATE TABLE ledgers (
    ledger_index integer NOT NULL,
    ledger_hash bytea,
    parent_hash bytea,
    total_coins bigint,
    close_time bigint,
    close_time_resolution bigint,
    accounts_hash bytea,
    transactions_hash bytea
);


ALTER TABLE public.ledgers OWNER TO matthew;

--
-- Name: transactions; Type: TABLE; Schema: public; Owner: matthew; Tablespace: 
--

CREATE TABLE transactions (
    tx_id bigint NOT NULL,
    tx_hash bytea,
    ledger_index bigint,
    tx_type text,
    account character varying(64),
    account_seq bigint,
    tx_seq integer,
    tx_result character varying(255),
    tx_raw bytea,
    tx_meta bytea,
    executed_time bigint,
    CONSTRAINT transactions_tx_type_check CHECK ((tx_type = ANY (ARRAY['Payment'::text, 'OfferCreate'::text, 'OfferCancel'::text, 'AccountSet'::text, 'SetRegularKey'::text, 'TrustSet'::text, 'EnableAmendment'::text, 'SetFee'::text])))
);


ALTER TABLE public.transactions OWNER TO matthew;

--
-- Name: transactions_tx_id_seq; Type: SEQUENCE; Schema: public; Owner: matthew
--

CREATE SEQUENCE transactions_tx_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.transactions_tx_id_seq OWNER TO matthew;

--
-- Name: transactions_tx_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: matthew
--

ALTER SEQUENCE transactions_tx_id_seq OWNED BY transactions.tx_id;


--
-- Name: account_id; Type: DEFAULT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY accounts ALTER COLUMN account_id SET DEFAULT nextval('accounts_account_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY knex_migrations ALTER COLUMN id SET DEFAULT nextval('knex_migrations_id_seq'::regclass);


--
-- Name: tx_id; Type: DEFAULT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY transactions ALTER COLUMN tx_id SET DEFAULT nextval('transactions_tx_id_seq'::regclass);


--
-- Data for Name: account_transactions; Type: TABLE DATA; Schema: public; Owner: matthew
--

COPY account_transactions (account_id, tx_id) FROM stdin;
\.


--
-- Data for Name: accounts; Type: TABLE DATA; Schema: public; Owner: matthew
--

COPY accounts (account_id, account, parent, tx_hash, created_time) FROM stdin;
\.


--
-- Name: accounts_account_id_seq; Type: SEQUENCE SET; Schema: public; Owner: matthew
--

SELECT pg_catalog.setval('accounts_account_id_seq', 1, false);


--
-- Data for Name: knex_migrations; Type: TABLE DATA; Schema: public; Owner: matthew
--

COPY knex_migrations (id, name, batch, migration_time) FROM stdin;
7	20140915173225_initial.js	1	2014-10-01 12:27:08.113-07
\.


--
-- Name: knex_migrations_id_seq; Type: SEQUENCE SET; Schema: public; Owner: matthew
--

SELECT pg_catalog.setval('knex_migrations_id_seq', 7, true);


--
-- Data for Name: ledgers; Type: TABLE DATA; Schema: public; Owner: matthew
--

COPY ledgers (ledger_index, ledger_hash, parent_hash, total_coins, close_time, close_time_resolution, accounts_hash, transactions_hash) FROM stdin;
\.


--
-- Data for Name: transactions; Type: TABLE DATA; Schema: public; Owner: matthew
--

COPY transactions (tx_id, tx_hash, ledger_index, tx_type, account, account_seq, tx_seq, tx_result, tx_raw, tx_meta, executed_time) FROM stdin;
\.


--
-- Name: transactions_tx_id_seq; Type: SEQUENCE SET; Schema: public; Owner: matthew
--

SELECT pg_catalog.setval('transactions_tx_id_seq', 1, false);


--
-- Name: accounts_account_unique; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY accounts
    ADD CONSTRAINT accounts_account_unique UNIQUE (account);


--
-- Name: accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);


--
-- Name: knex_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY knex_migrations
    ADD CONSTRAINT knex_migrations_pkey PRIMARY KEY (id);


--
-- Name: ledgers_ledger_hash_unique; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY ledgers
    ADD CONSTRAINT ledgers_ledger_hash_unique UNIQUE (ledger_hash);


--
-- Name: ledgers_ledger_index_unique; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY ledgers
    ADD CONSTRAINT ledgers_ledger_index_unique UNIQUE (ledger_index);


--
-- Name: ledgers_parent_hash_unique; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY ledgers
    ADD CONSTRAINT ledgers_parent_hash_unique UNIQUE (parent_hash);


--
-- Name: ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY ledgers
    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (ledger_index);


--
-- Name: transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (tx_id);


--
-- Name: transactions_tx_hash_unique; Type: CONSTRAINT; Schema: public; Owner: matthew; Tablespace: 
--

ALTER TABLE ONLY transactions
    ADD CONSTRAINT transactions_tx_hash_unique UNIQUE (tx_hash);


--
-- Name: account_transactions_account_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY account_transactions
    ADD CONSTRAINT account_transactions_account_id_foreign FOREIGN KEY (account_id) REFERENCES accounts(account_id);


--
-- Name: account_transactions_tx_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY account_transactions
    ADD CONSTRAINT account_transactions_tx_id_foreign FOREIGN KEY (tx_id) REFERENCES transactions(tx_id);


--
-- Name: transactions_ledger_index_foreign; Type: FK CONSTRAINT; Schema: public; Owner: matthew
--

ALTER TABLE ONLY transactions
    ADD CONSTRAINT transactions_ledger_index_foreign FOREIGN KEY (ledger_index) REFERENCES ledgers(ledger_index);


--
-- Name: public; Type: ACL; Schema: -; Owner: matthew
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM matthew;
GRANT ALL ON SCHEMA public TO matthew;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

