var config   = require('../../config/import.config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var utils    = require('../../storm/multilang/resources/src/lib/utils');

var types     = ['full', 'parsed'];
var LI_PAD    = 12;
var options   = config.get('hbase');
var type      = config.get('type') || 'full';
var prefix    = config.get('prefix') || options.prefix;
var start     = config.get('startIndex');
var end       = config.get('stopIndex');
var batchSize = config.get('batchSize') || 50;

var saved     = 0;
var counter   = 0;
var complete  = false;
var fetching  = false;

var stageOpts;
var hbase;
var stage;
var iterator;
var first;
var batch;

options.logLevel = 2;
stageOpts        = JSON.parse(JSON.stringify(options));
stageOpts.prefix = prefix;

//get connection to existing tables
hbase = new Hbase(options);

//get connection to new tables
stage = new Hbase(stageOpts);

//offset start index so that it is included
if (start) start += 1;
if (batchSize < 30) batchSize = 30;

iterator = hbase.iterator({
  table     : 'lu_ledgers_by_index',
  startRow  : utils.padNumber(start || 0, LI_PAD),
  stopRow   : utils.padNumber(end || 0, LI_PAD),
  batchSize : batchSize
});

function getNext() {
  fetching = true;

  iterator.getNext(function(err, resp) {
    fetching = false;

    if (err) {
      done(err);
      return;

    } else if (!resp.length) {
      console.log('no more ledgers');
      complete = true;
      done();
      return;
    }

    if (!first) {
      first = resp[0].ledger_index || resp[0].rowkey;
      console.log('FIRST:', first);
    }

    counter += resp.length;

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

    //parser expects ripple epoch
    ledger.close_time -= 946684800;

    //ledgers must be formatted according to the output from
    //rippled's ledger command
    ledger.transactions.forEach(function(tx, i) {
      var transaction      = tx.tx;
      transaction.metaData = tx.meta,
      transaction.hash     = tx.hash,
      ledger.transactions[i] = transaction;
    });

    if (type === 'full') {
      saveLedger(ledger);
    } else {
      saveParsedData(ledger);
    }
  });
}

function saveParsedData (ledger) {
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
                  resp + ' row(s)',
                  ledger.ledger_index,
                  'processed: '+ saved,
                  '---',
                  counter + ' pending');

    } else {
      console.log('no parsed data: ',
                  ledger.ledger_index,
                  'processed: '+ saved,
                  '---',
                  counter + ' pending');
    }

    if (counter > 20 && !fetching && !complete) {
      console.log('finished batch');
      getNext();
    }
  });
}

function saveLedger (ledger) {
  var parsed = Parser.parseLedger(ledger);

  stage.saveParsedData({data:parsed}, function(err, resp) {
    if (err) {
      console.log('unable to save parsed data for ledger: ' + ledger.ledger_index);
      done(err);
      return;
    }

    if (resp) console.log('parsed data saved: ',
                resp + ' row(s)',
                ledger.ledger_index);

    stage.saveTransactions(parsed.transactions, function(err, resp) {
      if (err) {
        console.log('unable to save transactions for ledger: ' + ledger.ledger_index);
        done(err);
        return;
      }

      if (resp) console.log(resp + ' transactions(s) saved:', ledger.ledger_index);

      stage.saveLedger(parsed.ledger, function(err, resp) {
        if (err) {
          console.log('unable to save ledger: ' + ledger.ledger_index);
          done(err);
          return;

        } else {

          saved++;
          counter--;

          console.log('ledger saved: ',
            ledger.ledger_index,
            '   saved: ' + saved,
            '---',
            counter + ' pending');

          if (counter < 20 && !fetching && !complete) {
            console.log('getting next batch');
            getNext();
          }
        }
      });
    });
  });
}

function done(err) {
  if (err) console.log(err);
  else if (counter) {
    setTimeout(done, 100);
    return;
  }

  process.exit();
}

getNext();
