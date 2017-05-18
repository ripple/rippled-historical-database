var config = require('../config');
var Logger = require('./logger');
var Parser = require('./ledgerParser');
var utils = require('./utils');
var validator = require('./validator');
var importer = require('./ripple-importer');
var hbase = require('./hbase')

/**
 * LedgerStream
 */

function LedgerStream() {
  var self = this;
  var logOpts = {
    scope: 'ledger-stream',
    file: config.get('logFile'),
    level: config.get('logLevel')
  };


  this.log = new Logger(logOpts);
  this.live = importer; // needed for storm
  this.validator = validator; // needed for storm
  this.hbase = hbase;

  this.ledgers = [];
  this.transactions = [];

  validator.on('ledger', function(ledger, callback) {
    self.ledgers.push({
      ledger: ledger,
      cb: callback
    });
  });

  importer.on('ledger', function(ledger) {
    self.ledgers.push({
      ledger: ledger,
      cb: null
    });
  });
};

//start live importer
LedgerStream.prototype.start = function () {
  validator.start();
  importer.liveStream();
};

//stop live importer
LedgerStream.prototype.stop = function () {
  validator.stop();
  importer.stop();
};

/**
 * processNextLedger
 */

LedgerStream.prototype.processNextLedger = function (callback) {
  var self   = this;
  var row    = this.ledgers.shift();
  var transactions;

  if (!row) {
    callback(); //nothing to do
    return;
  }

  //summarize fees
  row.feeSummary = Parser.summarizeFees(row.ledger);

  //replace transaction array
  //with array of hashes
  transactions = row.ledger.transactions;
  row.ledger.transactions = [];

  transactions.forEach(function(tx) {
    try {
      var prepared = self.prepareTransaction(row.ledger, tx);

    } catch (e) {
      self.log.error(e);
      callback('error preparing tx: ' + row.ledger.ledger_index + ' ' + tx.hash);
      return;
    }

    //add the transaction to
    //the processing queue
    self.transactions.push(prepared);
    row.ledger.transactions.push(tx.hash);
  });

  callback(null, row);
};

/**
 * prepareTransaction
 */

LedgerStream.prototype.prepareTransaction = function (ledger, tx) {
  tx.raw           = utils.toHex(tx);
  tx.meta          = utils.toHex(tx.metaData);

  tx.ledger_hash   = ledger.ledger_hash;
  tx.ledger_index  = ledger.ledger_index;
  tx.executed_time = ledger.close_time;
  tx.tx_index      = tx.metaData.TransactionIndex;
  tx.tx_result     = tx.metaData.TransactionResult;

  return tx;
};

module.exports = new LedgerStream();
