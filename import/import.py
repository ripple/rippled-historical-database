#!/usr/bin/env python3

__author__ = 'mtravis'

import pyripple
import psycopg2
import argparse
import json


def store_transaction(pgcursor, ledgers, transaction):
    t = {"id": None, "Account": None, "Destination": None, "Fee": None,
         "Flags": None, "Paths": None, "SendMax": None,
         "OfferSequence": None, "Sequence": None, "SigningPubKey": None,
         "TakerGets": None, "TakerPays": None, "TransactionType": None,
         "TxnSignature": None, "hash": None, "metaData": None}

    for key in t:
        if key in transaction:
            t[key] = transaction[key]

    sql = "INSERT INTO TRANSACTIONS VALUES (DEFAULT, %s, %s, %s, %s, %s, %s," +\
        "%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    print("TRANSACTION_TYPE: " + str(t["TransactionType"]))
    pgcursor.execute(sql, (t["Account"], t["Destination"], t["Fee"], t["Flags"],
                           t["Paths"], t["SendMax"], t["OfferSequence"],
                           t["Sequence"], t["SigningPubKey"],
                           json.dumps(t["TakerGets"]),
                           json.dumps(t["TakerPays"]), t["TransactionType"],
                           t["TxnSignature"], t["hash"],
                           json.dumps(t["metaData"])))
    sql = "SELECT currval(pg_get_serial_sequence('TRANSACTIONS', 'id'));"
    pgcursor.execute(sql)
    t["id"] = pgcursor.fetchone()[0]

    sql = "INSERT INTO LEDGER_TRANSACTIONS VALUES (%s, %s, %s);"
    pgcursor.execute(sql, (t["id"], ledgers["id"], t["Sequence"]))

    print("TRANSACTION: ")
    print(t)

#    if t["Account"] is not None:
#        sql = "INSERT INTO ACCOUNTS (address) VALUES (%s);"
#        pgcursor.execute(sql, (t["Account"],) )
#        pgcursor.execute("SELECT currval(pg_get_serial_sequence('ACCOUNTS',
# 'id'));")
#        account_id = pgcursor.fetchone()[0]
#        print(account_id)
#
#        sql = "INSERT INTO ACCOUNT_TRANSACTIONS VALUES (%s, %s, %s, %s);"
#        pgcursor.execute(sql, (t["id"], account_id, ledgers["seqNum"],
# t["Sequence"]))


def store_ledger(pghandle, ledgerobj):
    # 1 ledger record per ledger
    # 0+ transactions per ledger
    # [01] accounts per transaction
    # 1 account_transaction per transaction
    # 1 ledger_transaction per transaction
    pgcursor = pghandle.cursor()
    pgcursor.execute("BEGIN;")
    ledgers = {"id": None, "ledger_hash": None, "parent_hash": None,
               "total_coins": None, "close_time": None,
               "close_time_resolution": None, "account_hash": None,
               "transaction_hash": None, "accepted": None, "closed": None,
               "close_time_estimated": None, "close_time_human": None,
               "seqNum": None}

    for key in ledgers.keys():
        if key in ledgerobj:
            ledgers[key] = ledgerobj[key]
    sql = "INSERT INTO LEDGERS VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s," +\
        "%s, %s, %s, %s);"
    pgcursor.execute(sql, (ledgers["ledger_hash"], ledgers["parent_hash"],
                           ledgers["total_coins"], ledgers["close_time"],
                           ledgers["close_time_resolution"],
                           ledgers["account_hash"], ledgers["transaction_hash"],
                           ledgers["accepted"], ledgers["closed"],
                           ledgers["close_time_estimated"],
                           ledgers["close_time_human"]))
    pgcursor.execute("SELECT currval(pg_get_serial_sequence('LEDGERS', 'id'));")

    print("------------")
    ledgers["id"] = pgcursor.fetchone()[0]
    print(ledgers)

    for transaction in ledgerobj["transactions"]:
        store_transaction(pgcursor, ledgers, transaction)

    pgcursor.execute("COMMIT;")
    pgcursor.close()

# main
argparser = argparse.ArgumentParser()
argparser.add_argument("-c", "--connection", type=str, required=True)
argparser.add_argument("-p" "--pgconnection", type=str, required=True)
argparser.add_argument("-s", "--start", type=int, required=True)
argparser.add_argument("-e", "--end", type=int)
argparser.add_argument("-t", "--timeout", type=int, default=60)
args = argparser.parse_args()

if args.end is None:
    args.end = args.start

ripd = pyripple.Ripple(args.connection, timeout=args.timeout)
pgconn = psycopg2.connect(args.p__pgconnection)
pgconn.set_session(autocommit=False)

for i in range(args.start, args.end+1):
    print(i)
    ledger = ripd.cmd_ledger(args.start, transactions=True, expand=True)
    print(ledger)
    store_ledger(pgconn, ledger["result"]["ledger"])
