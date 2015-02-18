//get validated index from the control table
//if it doesnt exist, use GENESIS_LEDGER - 1
//fetch validated ledger_hash
//fetch ledger at index validated + 1
//fetch last validated index from rippled
//get transactions
//if all the data is there, increment validated
//otherwise, fetch from rippled
//save data, increment validated
//repeat until rippled validated is reached

var Importer = require('./modules/ripple-importer');
var Logger   = require('./modules/logger');
var Hbase    = require('./hbase-client');
var utils    = require('./utils');
var ripple   = require('ripple-lib');
var events   = require('events');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var Validator = function (config) {
  var self     = this;
  var importer = new Importer({ripple:config.ripple});
  var hbase    = new Hbase(config.hbase);
  var log      = new Logger({
    scope : 'validator',
    level : config.logLevel || 0,
    file  : config.logFile
  });
  
  var max;
  var lastValid;
  var working;
  var timer;
  
  hbase.connect();
  
  this.start = function () {
    
    if (!hbase.isConnected()) {
      setTimeout(function() {
        self.start();
      }, 1000);
    }
    
    if (!timer) {
      timer = setTimeout(function() {
        startValidation(); 
      }, 60*1000);
    }
    
    working = false;
    startValidation();
  }
  
  this.stop = function () {
    if (timer) {
      clearTimeout(timer);
    }
    
    working = false;
  }
  

  /**
   * startValidation
   */

  function startValidation() {
    
    if (working) return;
    working = true;
    
    log.info('starting validation process');
    hbase.getRow('control', 'last_validated', function (err, ledger) {

      if (err) {
        log.error(err);
        working = false;
        return;
      }

      lastValid = ledger ? ledger : { };
      if (!lastValid.ledger_index) lastValid.ledger_index = GENESIS_LEDGER - 1;

      lastValid.ledger_index = parseInt(lastValid.ledger_index, 10);

      log.info('Last valid index:', lastValid.ledger_index);

      //get latest ledger index
      importer.getLedger({
        index        : 'validated', 
        expand       : false,
        transactions : false
      }, function (err, resp) {
        if (err) {
          log.error(err);
          working = false;
          return;
        }

        max = parseInt(resp.ledger_index, 10);   
        log.info('latest validated ledger index:', max);
        checkNextLedger();
      });
    });
  }
  

  /**
   * checkNextLedger
   */

  function checkNextLedger () {
    var txHash;

    hbase.getLedger({
      ledger_index : lastValid.ledger_index + 1,
      transactions : true

    }, function (err, ledger) {

      if (err) {
        log.error(err);
        working = false;
        return;
      }

      //TODO: get missing ledger
      if (!ledger) {
        log.info('missing ledger', lastValid.ledger_index + 1);
        importLedger(lastValid.ledger_index + 1);
        return;
      }

      //form transactions for hash calc
      ledger.transactions.forEach(function(tx, i) {
        var transaction = tx.tx;
        transaction.metaData = tx.meta;
        transaction.hash = tx.hash;
        ledger.transactions[i] = transaction;
      });

      //make sure the hash of the
      //transactions is accurate to the known result
      txHash = ripple.Ledger.from_json(ledger).calc_tx_hash().to_hex();

      if (txHash !== ledger.transaction_hash) {
        log.error('transactions do not hash to the expected value for ' + 
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'ledger_hash: ' + ledger.ledger_hash + '\n' +
          'actual transaction_hash:   ' + txHash + '\n' +
          'expected transaction_hash: ' + ledger.transaction_hash);  
          importLedger(lastValid.ledger_index + 1);
          return;
        
      //make sure the hash chain is intact 
      } else if (lastValid.ledger_hash && lastValid.ledger_hash != ledger.parent_hash) {
        log.error('incorrect parent_hash:\n' + 
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'parent_hash: ' + ledger.parent_hash + '\n' +
          'expected: ' + lastValid.ledger_hash); 
        
        if (lastValid.ledger_index - 1 > GENESIS_LEDGER) {
          lastValid.ledger_index--;
          lastValid.parent_hash = null;
          lastValid.ledger_hash = null;

          setImmediate(function() {
            checkNextLedger();
          });  
        }
        
      //update last validated index in hbase
      } else {
        updateLastValid(ledger);
      }
    });
  }
  
  /**
   * updateLastValid
   */
  
  function updateLastValid (ledger) {
    var valid = {
      ledger_index : ledger.ledger_index,
      ledger_hash  : ledger.ledger_hash,
      parent_hash  : ledger.parent_hash
    };

    hbase.putRow('control', 'last_validated', valid)
    .nodeify(function(err, resp) {

      if (err) {
        log.error(err);
        working = false;
        return;
      } 
      
      lastValid = valid;
      log.info('last valid index advanced to', lastValid.ledger_index);

      if (lastValid.ledger_index < max) {
        setImmediate(function() {
          checkNextLedger();
        });

      } else {
        log.info('reached max:', max);
        working = false;
      }
    });
  }
  
  /**
   * importLedger
   */
  
  function importLedger (ledger_index, callback) {

    importer.getLedger({index : ledger_index}, function (err, ledger) {
      if (err) {
        log.error(err);
        working = false;
        return;
      }

      log.info('got ledger:', ledger.ledger_index);
      self.emit('ledger', ledger, function(err, resp) {
        
        if (err) {
          log.error(err);
          working = false;
          return;
        }
        
        setImmediate(function() {
          checkNextLedger();
        });  
      });
    });  
  }
};

Validator.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = Validator;