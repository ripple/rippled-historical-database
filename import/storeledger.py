__author__ = 'mtravis'

import psycopg2
import json


class StoreLedger:
    def __init__(self, pghandle, ledgerdict):
        self._pghandle = pghandle
        self._accounts = list()

        if "transactions" in ledgerdict:
            self.create_accounts(ledgerdict["transactions"])

        try:
            pgcursor = self._pghandle.cursor()
            pgcursor.execute("BEGIN;")
            inledger = {"id": None, "ledger_hash": None, "parent_hash": None,
                        "total_coins": None, "close_time": None,
                        "close_time_resolution": None, "account_hash": None,
                        "transaction_hash": None, "accepted": None, "closed": None,
                        "close_time_estimated": None, "close_time_human": None}

            for key in inledger.keys():
                if key in ledgerdict:
                    inledger[key] = ledgerdict[key]
            sql = "INSERT INTO LEDGERS VALUES (DEFAULT, %s, %s, %s, %s, %s, %s," +\
                "%s, %s, %s, %s, %s);"
            pgcursor.execute(sql, (inledger["ledger_hash"], inledger["parent_hash"],
                                   inledger["total_coins"], inledger["close_time"],
                                   inledger["close_time_resolution"],
                                   inledger["account_hash"],
                                   inledger["transaction_hash"],
                                   inledger["accepted"], inledger["closed"],
                                   inledger["close_time_estimated"],
                                   inledger["close_time_human"]))
            sql = "SELECT currval(pg_get_serial_sequence('LEDGERS', 'id'));"
            pgcursor.execute(sql)
            inledger["id"] = pgcursor.fetchone()[0]

            if "transactions" in ledgerdict:
                for idx, transaction in enumerate(ledgerdict["transactions"]):
                    self.store_transaction(pgcursor, transaction,
                                           ledgerdict["seqNum"],
                                           inledger["id"],
                                           self._accounts[idx]["id"])

            pgcursor.execute("COMMIT;")
        except:
            pgcursor.execute("ROLLBACK;")
            raise

    def create_accounts(self, transactions):
        self._accounts = [dict() for _ in
                          range(len(transactions))]

        for idx, value in enumerate(transactions):
            if "Account" not in value:
                continue
            self._accounts[idx] = {"address": value["Account"],
                                   "id": self.get_account_id(value["Account"])}

    def get_account_id(self, address):
        pgcursor = self._pghandle.cursor()
        account_id = None

        while account_id is None:
            pgcursor.execute("SELECT id FROM accounts WHERE address = %s;",
                             [address])
            res = pgcursor.fetchone()
            if res is not None:
                account_id = res[0]
            else:
                try:
                    sql = "INSERT INTO accounts VALUES (DEFAULT, %s);"
                    pgcursor.execute(sql, [address])
                except psycopg2.IntegrityError:
                    pass

        return account_id

    @staticmethod
    def store_transaction(pgcursor, transaction, ledger_seq, ledger_id,
                          account_id):
        intransaction = {"id": None, "Account": None, "Destination": None,
                         "Fee": None, "Flags": None, "Paths": None,
                         "SendMax": None, "OfferSequence": None,
                         "Sequence": None, "SigningPubKey": None,
                         "TakerGets": None, "TakerPays": None,
                         "TransactionType": None,
                         "TxnSignature": None, "hash": None, "metaData": None}

        for key in intransaction:
            if key in transaction:
                intransaction[key] = transaction[key]

        sql = "INSERT INTO TRANSACTIONS VALUES (DEFAULT, %s, %s, %s, %s, " +\
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        pgcursor.execute(sql, (intransaction["Account"],
                               intransaction["Destination"],
                               intransaction["Fee"], intransaction["Flags"],
                               json.dumps(intransaction["Paths"]),
                               json.dumps(intransaction["SendMax"]),
                               intransaction["OfferSequence"],
                               intransaction["Sequence"],
                               intransaction["SigningPubKey"],
                               json.dumps(intransaction["TakerGets"]),
                               json.dumps(intransaction["TakerPays"]),
                               intransaction["TransactionType"],
                               intransaction["TxnSignature"],
                               intransaction["hash"],
                               json.dumps(intransaction["metaData"])))
        sql = "SELECT currval(pg_get_serial_sequence('TRANSACTIONS', 'id'));"
        pgcursor.execute(sql)
        intransaction["id"] = pgcursor.fetchone()[0]

        sql = "INSERT INTO LEDGER_TRANSACTIONS VALUES (%s, %s, %s);"
        pgcursor.execute(sql, (intransaction["id"], ledger_id,
                               intransaction["Sequence"]))

        if intransaction["Account"] is not None:
            sql = "INSERT INTO ACCOUNT_TRANSACTIONS VALUES (%s, %s, %s, %s);"
            pgcursor.execute(sql, [intransaction["id"], account_id, ledger_seq,
                                   intransaction["Sequence"]])