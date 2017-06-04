var config   = require('../config/import.config');
var Logger   = require('../lib/logger');
var Hbase    = require('../lib/hbase');
var Parser   = require('../lib/ledgerParser');
var utils    = require('../lib/utils');

var types     = ['full', 'parsed'];
var LI_PAD    = 12;
var options   = config.get('hbase');
var type      = config.get('type') || '';
var prefix    = 'prod_';
var start     = config.get('startIndex');
var stop      = config.get('stopIndex');
var batchSize = config.get('batchSize') || 20;

var saved     = 0;
var counter   = 0;
var complete  = false;
var fetching  = false;
var t         = Date.now();

var originOpts;
var origin;
var hbase;
var iterator;
var first;
var batch;
var min;

options.logLevel  = 2;
originOpts        = JSON.parse(JSON.stringify(options));
originOpts.prefix = prefix;
originOpts.logLevel = 1;

//get connection to origin tables
origin = new Hbase(originOpts);

//get connection to new tables
hbase = new Hbase(options);

//offset stop index so that it is included
if (stop) stop += 1;
if (batchSize < 5) batchSize = 5;
min = batchSize > 20 ? batchSize * 5.5 : 100;

iterator = origin.iterator({
  table      : 'lu_ledgers_by_index',
  startRow   : utils.padNumber(start || 0, LI_PAD),
  stopRow    : utils.padNumber(stop  || 900000000, LI_PAD),
  descending : false,
  count      : batchSize,
  caching    : config.get('cache') || 30
});

function getNext(cb) {
  fetching = true;

  iterator.getNext(function(err, resp) {

    fetching = false;

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

    counter += resp.length;
    console.log('got batch:', resp.length + ' ledgers');

    for (var i=0; i<resp.length; i++) {
      processLedger(resp[i]);
    }

    if (cb) cb();
  });
}

function processLedger(l) {

  origin.getLedger({
    ledger_hash: l.ledger_hash,
    transactions: true,
    expand: true
  }, function(err, ledger) {
    if (err || !ledger) {
      console.log(err, l.rowkey);
      counter--;
      done(err + ' ' + l.ledger_index);
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
  hbase.saveParsedData({
    data: parsed,
    tableNames: ['exchanges']
  }, function(err, resp) {
    counter--;

    if (err) {
      console.log('unable to save parsed data for ledger: ' + ledger.ledger_index);
      done(err + ' ' + ledger.ledger_index);
      return;
    }

    saved++;

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

    if (counter < min && !fetching && !complete) {
      console.log('finished batch');
      getNext();
    }
  });
}

function saveLedger (ledger) {
  var parsed = Parser.parseLedger(ledger);

  hbase.saveParsedData({data:parsed}, function(err, resp) {
    if (err) {
      console.log('unable to save parsed data for ledger: ' + ledger.ledger_index);
      counter--;
      done(err + ' ' + ledger.ledger_index);
      return;
    }

    if (resp) console.log('parsed data saved: ',
                resp + ' row(s)',
                ledger.ledger_index);

    hbase.saveTransactions(parsed.transactions, function(err, resp) {
      if (err) {
        console.log('unable to save transactions for ledger: ' + ledger.ledger_index);
        counter--;
        done(err + ' ' + ledger.ledger_index);
        return;
      }

      if (resp) console.log(resp + ' transactions(s) saved:', ledger.ledger_index);

      hbase.saveLedger(parsed.ledger, function(err, resp) {
        if (err) {
          console.log('unable to save ledger: ' + ledger.ledger_index);
          counter--;
          done(err + ' ' + ledger.ledger_index);
          return;

        } else {

          saved++;
          counter--;

          console.log('ledger saved: ',
            ledger.ledger_index,
            '   saved: ' + saved,
            '---',
            counter + ' pending');

          if (counter < min && !fetching && !complete) {
            console.log('getting next batch');
            getNext();
          }
        }
      });
    });
  });
}

function done(err) {
  complete = true;

  if (counter) {
    setTimeout(function(){
      done(err);
    }, 1000);
    return;
  }

  if (err) console.log(err);
  t = (Date.now() - t)/1000;
  var duration;
  if (t > 60*60*2)  duration = {time:t/3600, interval:'hour'};
  else if (t > 600) duration = {time:t/60, interval:'min'};
  else              duration = {time:t,interval:'sec'};

  console.log(duration.time + ' ' + duration.interval,
              saved + ' ledgers',
              t/saved + ' secs/ledger');

  process.exit();
}

//get first 4 batches immediately
console.log('getting first ledger batch');
getNext(function(){
  console.log('getting next batch');
  getNext(function(){
    console.log('getting next batch');
    getNext(function(){
      console.log('getting next batch');
      getNext();
    });
  });
});
