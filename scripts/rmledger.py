#!/usr/bin/env python3

import psycopg2
import argparse
import sys
import binascii

dsn = 'dbname=SET_THIS'

argparser = argparse.ArgumentParser()
argparser.add_argument('-l', '--ledger', type=int, required=True)
args = argparser.parse_args()

pg = psycopg2.connect(dsn)
cur = pg.cursor()
pg2 = psycopg2.connect(dsn)
cur2 = pg2.cursor()
pg3 = psycopg2.connect(dsn)
cur3 = pg2.cursor()

cur.execute('SELECT ledger_hash FROM ledgers WHERE ledger_index = %s;', (args.ledger,))
for h in cur:
    cur2.execute('SELECT tx_hash FROM transactions WHERE ledger_hash = %s;', (h[0],))
    for t in cur2:
        cur3.execute('DELETE from account_transactions WHERE tx_hash = %s;', (t[0],))
    cur2.execute('DELETE FROM transactions WHERE ledger_hash = %s;', (h[0],))
    pg2.commit()
cur.execute('DELETE FROM ledgers WHERE ledger_index = %s;', (args.ledger,))
pg.commit()

