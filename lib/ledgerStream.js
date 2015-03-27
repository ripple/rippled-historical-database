var Importer  = require('./ripple-importer');
var Logger    = require('./logger');
var utils     = require('./utils');
var Hbase     = require('./hbase/hbase-client');
var Validator = require('./validator');

var EPOCH_OFFSET = 946684800;

/**
 * LedgerStream
 */

function LedgerStream(options) {
  var self    = this;
  var logOpts = {
    scope : 'ledger-stream',
    file  : options.logFile,
    level : options.logLevel
  };

  var validateOpts = {
    logFile  : options.logFile ? 'validator.log' : null,
    logLevel : options.logLevel,
    ripple   : options.ripple,
    hbase    : options.hbase
  };

  options.hbase.logLevel = options.logLevel;
  options.hbase.logFile  = options.logFile;
  this.validator    = new Validator(validateOpts);
  this.hbase        = new Hbase(options.hbase);
  this.live         = new Importer(options);
  this.log          = new Logger(logOpts);
  this.ledgers      = [];
  this.transactions = [];

  this.validator.on('ledger', function (ledger, callback) {
    self.ledgers.push({ledger:ledger, cb:callback});
  });

  this.live.on('ledger', function (ledger) {
    self.ledgers.push({ledger:ledger, cb:null});
  });
};

//start live importer
LedgerStream.prototype.start = function () {
  this.validator.start();
  this.live.liveStream();
};

//stop live importer
LedgerStream.prototype.stop = function () {
  this.validator.stop();
  this.live.stop();
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

  //adjust the close time to unix epoch
  row.ledger.close_time = row.ledger.close_time + EPOCH_OFFSET;

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

module.exports = LedgerStream;
