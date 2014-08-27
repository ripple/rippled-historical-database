#!/usr/bin/env python3

__author__ = 'mtravis'

import argparse
import psycopg2
import pyripple
import storeledger
import time
import datetime
import sys

# main
argparser = argparse.ArgumentParser()
argparser.add_argument("-c", "--connection", type=str, required=True)
argparser.add_argument("-p" "--pgconnection", type=str, required=True)
argparser.add_argument("-l", "--logfile", type=str, required=True)
argparser.add_argument("-i", "--inputfile", type=str)
argparser.add_argument("-s", "--start", type=int)
argparser.add_argument("-e", "--end", type=int)
argparser.add_argument("-t", "--timeout", type=int, default=60)
args = argparser.parse_args()

if args.end is None:
    args.end = args.start

ripd = pyripple.Ripple(args.connection, timeout=args.timeout)
pgconn = psycopg2.connect(args.p__pgconnection)
pgconn.set_session(autocommit=True)

logfile = open(args.logfile, "a", 1)

if hasattr(args, "inputfile"):
    with open(args.inputfile, "r") as infile:
        for i in infile:
            i = i.rstrip()

            breakout = False
            while True:
                if breakout:
                    break;

                logfile.write(str(datetime.datetime.now()) + " fetching ledger " + i\
                    + "\n")
                ledger = None
                try:
                    ledger = ripd.cmd_ledger(i, transactions=True, expand=True)
                    logfile.write(str(datetime.datetime.now()) +
                              " fetched ledger " + str(i) + "\n")
                    break;
                except pyripple.RippleError as err:
                    logfile.write(str(err) + "\n")
                    logfile.write(str(sys.exc_info()) + "\n")
                    if err.error == "lgrNotFound":
                        breakout = True
                        continue # log this, go to next
                    time.sleep(2)
                except Exception as err:
                    if hasattr(err, "remote_ip"):
                        logfile.write("remote_ip: " + err.remote_ip)
                    logfile.write(str(err) + "\n")
                    logfile.write(str(sys.exc_info()) + "\n")
                    time.sleep(20) # log & stuff & try again, must be socket-related

            if breakout == True:
                continue

            logfile.write(str(datetime.datetime.now()) + " storing ledger " + i\
                + "\n")
            try:
                sl = storeledger.StoreLedger(pgconn, ledger["result"]["ledger"])
                logfile.write(str(datetime.datetime.now()) + " stored ledger "
                              + i + "\n")
            except:
                logfile.write(str(sys.exc_info()) + "\n")
                time.sleep(2)

    sys.exit(0)

for i in range(args.start, args.end+1):
    logfile.write(str(datetime.datetime.now()) + " fetching ledger " + str(i)\
        + "\n")
    try:
        ledger = ripd.cmd_ledger(i, transactions=True, expand=True)
    except pyripple.RippleError as err:
        if err.error == "lgrNotFound":
            continue # log this, go to next
        logfile.write(str(err) + "\n")
        logfile.write(str(sys.exc_info()) + "\n")
        time.sleep(2)
    except:
        logfile.write(str(sys.exc_info()) + "\n")
        time.sleep(20) # log & stuff & try again, must be socket-related
    finally:
        logfile.write(str(datetime.datetime.now()) +
                      " fetched ledger " + str(i) + "\n")

    logfile.write(str(datetime.datetime.now()) + " storing ledger " + str(i)\
        + "\n")
    try:
        sl = storeledger.StoreLedger(pgconn, ledger["result"]["ledger"])
    except:
        logfile.write(str(sys.exc_info()) + "\n")
        time.sleep(2)
    finally:
        logfile.write(str(datetime.datetime.now()) + " stored ledger "
                      + str(i) + "\n")
