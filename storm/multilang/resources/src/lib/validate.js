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
var Parser   = require('./modules/ledgerParser');
var Logger   = require('./modules/logger');
var Hbase    = require('./hbase-client');
var config   = require('../../config');
var utils    = require('./utils');
var ripple   = require('ripple-lib');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var importer = new Importer({ripple:config.ripple});
var hbase    = new Hbase(config.hbase);

var log = new Logger({
  scope : 'validator',
  level : config.logLevel || 0,
  //file  : config.logFile
});

var max;
var lastValid;

hbase.connect().then(function(){  
  startValidation();
});

/**
 * startValidation
 */

function startValidation() {
  log.info('starting validation process');
  hbase.getRow('control', 'last_validated', function (err, ledger) {

    if (err) {
      log.error(err);
      process.exit(1);
    }
    
    lastValid = ledger ? ledger : {
      ledger_index : GENESIS_LEDGER - 1,
      ledger_hash  : null,
      parent_hash  : null
    };
    
    lastValid.ledger_index = parseInt(lastValid.ledger_index, 10);
    
    log.info('Last valid index:', lastValid.ledger_index);
  
    //get latest ledger index
    importer.getLedger({
      index        : 'validated', 
      expand       : false,
      transactions : false
    }, function (err, resp) {
      if (err) {
        console.log(err);
        process.exit(1);
      }

      max = parseInt(resp.ledger_index, 10);   
      log.info('latest validated ledger index:', max);
      checkNextLedger(lastValid, max);
    });
  });
}

/**
 * checkNextLedger
 */

function checkNextLedger (lastValid, max) {
  var txHash;
  
  hbase.getLedger({
    ledger_index : lastValid.ledger_index + 1,
    transactions : true

  }, function (err, ledger) {
    
    if (err) {
      log.error(err);
      return;
    }
    
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

    //make sure the hash chain is intact  
    } else if (lastValid.ledger_hash && lastValid.ledger_hash != ledger.parent_hash) {
      log.error('incorrect parent_hash:\n' + 
        'ledger_index: ' + ledger.ledger_index + '\n' +
        'parent_hash: ' + ledger.parent_hash + '\n' +
        'expected: ' + lastValid.ledger_hash); 
      
    //update last validated index in hbase
    } else {
      lastValid = {
        ledger_index : ledger.ledger_index,
        ledger_hash  : ledger.ledger_hash,
        parent_hash  : ledger.parent_hash
      };
      
      hbase.putRow('control', 'last_validated', lastValid)
      .nodeify(function(err, resp) {
        
        if (err) {
          log.error(err);
          
        } else {
          log.info('last valid index advanced to', ledger.ledger_index);
        }
        
        if (lastValid.ledger_index < max) {
          setImmediate(function() {
            checkNextLedger(lastValid, max);
          });
        
        } else {
          setTimeout(function() {
            startValidation();
          }, 60*1000);
        }
      });
    }
  });
}
