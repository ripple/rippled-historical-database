#!/usr/bin/env python3

__author__ = 'mtravis'

import argparse
import psycopg2
import pyripple
import storeledger

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
pgconn.set_session(autocommit=True)

for i in range(args.start, args.end+1):
#    print(i)
    ledger = ripd.cmd_ledger(args.start, transactions=True, expand=True)
#    print(ledger)
    sl = storeledger.StoreLedger(pgconn, ledger["result"]["ledger"])
