var Importer = require('./modules/ripple-importer');
var Logger   = require('./modules/logger');
var utils    = require('./utils');
var Hbase    = require('./hbase-client');


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
  
  options.hbase.logLevel = options.logLevel;
  options.hbase.logFile  = options.logFile;
  
  this.hbase        = new Hbase(options.hbase);
  this.live         = new Importer(options);
  this.log          = new Logger(logOpts);
  this.ledgers      = [];
  this.transactions = [];
  
  this.live.on('ledger', function (ledger) {    
    self.ledgers.push(ledger);
  });
  
  //establish connection to hbase
  self.hbase.connect(); 
};

//start live importer
LedgerStream.prototype.start = function () {
  this.live.liveStream();
};

//stop live importer
LedgerStream.prototype.stop = function () {
  this.live.stop();
};

/**
 * processNextLedger
 */

LedgerStream.prototype.processNextLedger = function (callback) {
  var self   = this;
  var ledger = this.ledgers.shift();
  var transactions;
  
  if (!ledger) {
    callback(); //nothing to do
    return;
  }
  
  //adjust the close time to unix epoch
  ledger.close_time = ledger.close_time + EPOCH_OFFSET;

  //replace transaction array 
  //with array of hashes
  transactions = ledger.transactions;
  ledger.transactions = [];  
  
  transactions.forEach(function(tx) {
    try {
      var prepared = self.prepareTransaction(ledger, tx); 
      
    } catch (e) {
      self.log.error(e);
      callback('error preparing tx: ' + ledger.ledger_index + ' ' + tx.hash);
      return;
    }
    
    //add the transaction to 
    //the processing queue
    self.transactions.push(prepared);
    ledger.transactions.push(tx.hash); 
  });
  
  callback(null, ledger);
};

/**
 * prepareTransaction
 */

LedgerStream.prototype.prepareTransaction = function (ledger, tx) {
  var meta = tx.metaData;
  delete tx.metaData;
    
  tx.raw           = utils.toHex(tx);
  tx.meta          = utils.toHex(meta);
  tx.metaData      = meta;
  
  tx.ledger_hash   = ledger.ledger_hash;
  tx.ledger_index  = ledger.ledger_index;
  tx.executed_time = ledger.close_time;
  tx.tx_index      = tx.metaData.TransactionIndex;
  tx.tx_result     = tx.metaData.TransactionResult;
  
  return tx;
};

module.exports = LedgerStream;
