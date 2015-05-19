'use strict';

var Importer = require('./ripple-importer');
var Logger = require('./logger');
var Hbase = require('./hbase/hbase-client');
var ripple = require('ripple-lib');
var events = require('events');
var moment = require('moment');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var Validator = function(config) {

  config.hbase.logLevel = 2;

  var self = this;
  var importer = new Importer({ripple: config.ripple});
  var hbase = new Hbase(config.hbase);
  var log = new Logger({
    scope: 'validator',
    level: config.logLevel || 0,
    file: config.logFile
  });

  var max;
  var lastValid;
  var working;
  var timer;
  var startIndex = config.start;

  if (startIndex && startIndex < GENESIS_LEDGER) {
    startIndex = GENESIS_LEDGER - 1;
  }

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
    hbase.getLastValidated(function(err, ledger) {

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

      //stay one back so as not to
      //overrun the importer
      max = parseInt(resp.ledger_index, 10) - 1;
      log.info('latest validated ledger index:', max);

      if (lastValid.ledger_index >= max) {
        log.info('reached max:', max);
      } else {
        checkNextLedger();
      }
    });
  }


  /**
   * checkNextLedger
   */

  function checkNextLedger () {
    var txHash;

    hbase.getLedger({
      ledger_index: lastValid.ledger_index + 1,
      transactions: true,
      expand: true

    }, function (err, ledger) {

      //re-import the ledger if
      //a transaction is missing
      if (err && err === 'missing transaction') {
        log.info('ledger missing transaction', lastValid.ledger_index + 1);
        importLedger(lastValid.ledger_index + 1);
        return;

      } else if (err) {
        log.error(err);
        working = false;
        return;
      }

      if (!ledger) {
        log.info('missing ledger', lastValid.ledger_index + 1);
        importLedger(lastValid.ledger_index + 1);
        return;

      } else {
        for(var i=0; i<ledger.transactions.length; i++) {
          if (ledger.transactions[i].ledger_index !== ledger.ledger_index ||
              ledger.transactions[i].ledger_hash !== ledger.ledger_hash) {
            log.info('transaction refers to incorrect ledger', ledger.ledger_index);
            log.info('tx', ledger.transactions[i].ledger_index, ledger.transactions[i].ledger_hash);
            log.info('ledger', ledger.ledger_index, ledger.ledger_hash);
            importLedger(lastValid.ledger_index + 1);
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
      ledger_index: ledger.ledger_index,
      ledger_hash: ledger.ledger_hash,
      parent_hash: ledger.parent_hash,
      close_time: moment.unix(ledger.close_time).utc().format()
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

    log.info('importing ledger:', ledger_index);
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
