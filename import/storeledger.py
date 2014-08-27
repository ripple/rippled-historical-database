__author__ = 'mtravis'

import psycopg2
import time
import pyripple


class StoreLedger:
    def __init__(self, pghandle, ledgerdict):
        self._pghandle = pghandle
        self._accounts = list()

        if "transactions" in ledgerdict:
            self.create_accounts(ledgerdict["transactions"])

    def create_accounts(self, transactions):
        self._accounts = [dict() for _ in
                          range(len(transactions))]

        for idx, value in enumerate(transactions):
            if "Account" not in value:
                continue
            self._accounts[idx] = {"address": value["Account"],
                                   "id": self.get_account_id(value["Account"])}

        print(self._accounts)

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