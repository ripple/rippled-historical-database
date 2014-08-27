#!/usr/bin/env perl

# feed with:
# shut down rippled, copy ledger.db elsewhere, restart rippled
# sqlite3 -csv ledger.db "SELECT LedgerSeq, LedgerHash, PrevHash FROM Ledgers ORDER BY LedgerSeq ASC;"

$previous_seq = 0;
$previous_hash = "";

$seq = 0;

while(<STDIN>) {
    chomp($_);
    ($seq, $ledger_hash, my $parent_hash) = split(/,/, $_);
    if ($seq != $previous_seq + 1) {
        print "gap: $seq, $previous_seq\n";
    }
    $previous_seq = $seq;

    if ($parent_hash ne $previous_hash) {
        print "bad parent: $seq, $ledger_hash\n";
    }
    $previous_hash = $ledger_hash;
}

print "final ledger $seq, $ledger_hash\n";
