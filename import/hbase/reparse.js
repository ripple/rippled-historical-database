var config   = require('../../config/import.config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var utils    = require('../../storm/multilang/resources/src/lib/utils');

var LI_PAD  = 12;
var options = config.get('hbase');
var hbase;
var iterator;
var first;
var counter;
var total;
var batch;
var saved = 0;

options.logLevel = 2;

hbase = new Hbase(options);
options.prefix = 'stage_';
stage = new Hbase(options);

var start = config.get('startIndex');
var end   = config.get('stopIndex');

//offset start index so that it is included
if (start) start += 1;

iterator = hbase.iterator({
  table     : 'lu_ledgers_by_index',
  startRow  : utils.padNumber(start || 0, LI_PAD),
  stopRow   : utils.padNumber(end || 0, LI_PAD),
  batchSize : 500
});

function getNext() {
  iterator.getNext(function(err, resp) {

    if (err) {
      done(err);
      return;

    } else if (!resp.length) {
      console.log('no more ledgers');
      done();
      return;
    }

    if (!first) {
      first = resp[0].ledger_index || resp[0].rowkey;
      console.log('FIRST:', first);
    }

    total = counter = resp.length;

    for (var i=0; i<resp.length; i++) {
      processLedger(resp[i]);
    }
  });
}

function processLedger(l) {

  hbase.getLedger({ledger_hash : l.ledger_hash, transactions:true}, function(err, ledger) {
    if (err) {
      console.log(err, l.rowkey);
      done(err);
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

    //save to staging tables
    stage.saveParsedData({data:parsed}, function(err, resp) {
      if (err) {
        console.log('unable to save parsed data for ledger: ' + ledger.ledger_index);
        done(err);
        return;
      }

      saved++;
      counter--;

      if (resp) {
        console.log('parsed data saved: ',
                    resp + ' rows',
                    ledger.ledger_index,
                    'processed: '+ saved,
                    '---',
                    counter + ' of ' + total + ' remaining');

      } else {
        console.log('no parsed data: ',
                    ledger.ledger_index,
                    'processed: '+ saved,
                    '---',
                    counter + ' of ' + total + ' remaining');
      }

      if (!counter) {
        console.log('finished batch');
        getNext();
      }
    });
  });
}

function done(err) {
  if (err) console.log(err);
  process.exit();
}

getNext();
