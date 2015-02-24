var config   = require('../../config/import.config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var utils    = require('../../storm/multilang/resources/src/lib/utils');

var LI_PAD  = 12;
var options = config.get('hbase');
var pending = 0;
var saved   = 0;
var hbase;
var iterator;
var stop;
var stopping;
var first;

options.logLevel = 2;
options.timeout  = 10000;

hbase = new Hbase(options);

iterator = hbase.iterator({
  table    : 'lu_ledgers_by_index',
  startRow : utils.padNumber(config.get('startIndex') || '0', LI_PAD),
  stopRow  : utils.padNumber(config.get('stopIndex')  || '0', LI_PAD),
});

function getNext() {

  if (!stop && pending < 500) {

    pending++;

    iterator.getNext(function(err, resp) {

      if (err) {
        console.log(err);
        done();
        return;

      } else if (!resp) {
        console.log('no more ledgers');
        stop = true;
        pending--;
        return;
      }

      if (!first) {
        first = resp.ledger_index || resp.rowkey;
        console.log('FIRST:', first);
      }

      hbase.getLedger({ledger_hash : resp.ledger_hash, transactions:true}, function(err, ledger) {
        if (err) {
          console.log(err, resp.rowkey);
          done();
          return;
        }

        //ledgers must be formatted according to the output from
        //rippled's ledger command
        ledger.transactions.forEach(function(tx, i) {
          var transaction      = tx.tx;
          transaction.metaData = tx.meta,
          transaction.hash     = tx.hash
          ledger.transactions[i] = transaction;
          ledger.close_time   -= 946684800 //remove EPOCH OFFSET;
        });

        var parsed = Parser.parseLedger(ledger);

        hbase.saveParsedData({data:parsed}, function(err, resp) {
          if (err) {
            console.log('unable to save parsed data for ledger: ' + ledger.ledger_index);
            done();
            return;
          }

          pending--;
          saved++;

          console.log('parsed data saved: ', ledger.ledger_index, saved, pending);

          if (stop && !pending) {
            console.log('complete');
            process.exit();
          }
        });
      });
    });

  } else if (stop && !pending) {
    done();
    return;
  }

  setTimeout(getNext, 5);
}

function done (close) {
  if (stopping) return;
  console.log('stopping');
  stopping = true;
  //clearInterval(interval);
  iterator.close();

  console.log(saved, pending, first);
}

getNext();
