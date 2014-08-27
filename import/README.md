This directory has scripts and notes for doing importation and data validation
from rippled

Files:
rippled_history_schema.sql:
schema created by "pg_dump -s rippled_history"

validate.pl:
Validates ledger parent and children in SQLite line up, and that there are no
sequence gaps.
1. shut down rippled, copy ledger.db elsewhere, restart rippled
2. sqlite3 -csv ledger.db "SELECT LedgerSeq, LedgerHash, PrevHash FROM Ledgers ORDER BY LedgerSeq ASC;"
> <somefile>
3. ./validate.pl < <somefile>


