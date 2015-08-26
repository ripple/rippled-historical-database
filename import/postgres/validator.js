var config   = require('../../config/import.config');
var Logger   = require('../../lib/logger');
var Importer = require('../../lib/ripple-importer');
var utils    = require('../../lib/utils');
var Postgres = require('./client');
var ripple   = require('ripple-lib');
var events   = require('events');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var Validator = function (config) {
  var self     = this;
  var importer = new Importer({ripple:config.ripple});
  var db       = new Postgres(config.postgres);
  var log      = new Logger({
    scope : 'validator',
    level : config.logLevel || 0,
    file  : config.logFile
  });

  var max;
  var lastValid;
  var working;
  var timer;
  var startIndex = config.start;

  this.start = function () {

    if (!timer && !startIndex) {
      timer = setInterval(function() {
        startValidation();
      }, 30*1000);
    }

    working = false;
    startValidation();
  }

  this.stop = function () {
    log.info('validation process stopped');
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

    if (startIndex) {
      lastValid = {
        ledger_index : startIndex
      };
      getLatestIndex();
      return;
    }

    log.info('starting validation process');

    db.getLastValidated(function(err, ledger) {

      if (err) {
        log.error(err);
        working = false;
        return;
      }

      lastValid = ledger ? ledger : { };
      if (!lastValid.ledger_index) lastValid.ledger_index = GENESIS_LEDGER - 1;

      lastValid.ledger_index = parseInt(lastValid.ledger_index, 10);

      log.info('Last valid index:', lastValid.ledger_index);

      getLatestIndex();
    });
  }

  function getLatestIndex () {

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

      max = parseInt(resp.ledger_index, 10) - 1;
      log.info('latest validated ledger index:', max);
      checkNextLedger();
    });
  }


  /**
   * checkNextLedger
   */

  function checkNextLedger () {
    var txHash;

    db.getLedger({
      ledger_index : lastValid.ledger_index + 1,
      tx_return : 'json'

    }, function (err, ledger) {

      if (err && err.error === 'ledger not found') {
        log.info('missing ledger', lastValid.ledger_index + 1);
        importLedger(lastValid.ledger_index + 1);
        return;

      } else if (err) {
        log.error(err);
        working = false;
        return;

      } else {

        for(var i=0; i<ledger.transactions.length; i++) {
          if (ledger.transactions[i].ledger_index !== ledger.ledger_index) {
            log.info('transaction refers to incorrect ledger', ledger.ledger_index);
            log.info('tx', ledger.transactions[i].ledger_index, ledger.transactions[i].ledger_hash);
            log.info('ledger', ledger.ledger_index, ledger.ledger_hash);

            //importLedger(lastValid.ledger_index + 1);
            self.stop();
            return;
          }
        }
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
      try {
        txHash = ripple._DEPRECATED.Ledger.from_json(ledger)
          .calc_tx_hash().to_hex();
      } catch(e) {
        log.error('hash calc error:', ledger.ledger_index, e.stack || e);
        self.stop();
        return;
      }


      if (txHash !== ledger.transactions_hash.toUpperCase()) {
        log.error('transactions do not hash to the expected value for ' +
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'ledger_hash: ' + ledger.ledger_hash + '\n' +
          'actual transaction_hash:   ' + txHash + '\n' +
          'expected transaction_hash: ' + ledger.transactions_hash);

        //importLedger(lastValid.ledger_index + 1);
        self.stop();
        return;

      //make sure the hash chain is intact
      } else if (lastValid.ledger_hash && lastValid.ledger_hash != ledger.parent_hash) {
        log.error('incorrect parent_hash:\n' +
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'parent_hash: ' + ledger.parent_hash + '\n' +
          'expected: ' + lastValid.ledger_hash);

        self.stop();
        return;
        /*
        if (lastValid.ledger_index - 1 > GENESIS_LEDGER) {
          lastValid.ledger_index--;
          lastValid.parent_hash = null;
          lastValid.ledger_hash = null;

          setImmediate(function() {
            checkNextLedger();
          });
        }
        */

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

    //dont save if startIndex is used
    if (startIndex) {
      lastValid = valid;
      log.info('valid', lastValid.ledger_index);
      if (lastValid.ledger_index < max) {
        setImmediate(function() {
          checkNextLedger();
        });

      } else {
        log.info('reached max:', max);
        working = false;
        if (startIndex) {
          process.exit();
        }
      }

      return;
    }

    db.setLastValidated(valid, function(err, resp) {

      if (err || !resp) {
        log.error(err, resp);
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

    log.info('importing ledger:', ledger_index);
    importer.getLedger({index : ledger_index}, function (err, ledger) {
      if (err) {
        log.error(err);
        working = false;
        return;
      }

      log.info('got ledger:', ledger.ledger_index);
      db.saveLedger(ledger, function(err, resp) {

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

//Validator.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = Validator;
