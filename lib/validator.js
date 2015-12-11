'use strict';

var Importer = require('./ripple-importer');
var Logger = require('./logger');
var Hbase = require('./hbase/hbase-client');
var ripple = require('ripple-lib');
var events = require('events');
var moment = require('moment');
var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport();
var rippleAPI = new ripple.RippleAPI();
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger
var EPOCH_OFFSET = 946684800;
/**
 * Validator class
 */

var Validator = function(config) {

  config.hbase.logLevel = 2;

  var self = this;
  var to = config.recipients || [];
  var notifications = {};
  var importer = new Importer(config);
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
        if (startIndex) {
          process.exit(1);
        } else {
          working = false;
          return;
        }
      }

      lastValid = ledger ? ledger : { };
      if (!lastValid.ledger_index) lastValid.ledger_index = GENESIS_LEDGER - 1;

      lastValid.ledger_index = parseInt(lastValid.ledger_index, 10);

      log.info('Last valid index:', lastValid.ledger_index);

      getLatestIndex();
    });
  }

  function getLatestIndex() {
    importer.getLedger(null, function (err, resp) {
      if (err) {
        log.error(err);

        if (startIndex) {
          process.exit(1);
        } else {
          working = false;
          return;
        }
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

  function checkNextLedger() {
    hbase.getLedger({
      ledger_index: lastValid.ledger_index + 1,
      transactions: true,
      expand: true,
      include_ledger_hash: true
    },
    function (err, ledger) {
      var hash;
      var converted;
      var message;

      //re-import the ledger if
      //a transaction is missing
      if (err && err.indexOf('missing transaction') !== -1) {
        log.info('ledger missing transaction', lastValid.ledger_index + 1);
        importLedger(lastValid.ledger_index + 1);
        return;

      } else if (err) {
        log.error(err);
        if (startIndex) {
          process.exit(1);
        } else {
          working = false;
          return;
        }
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

      //console.log(convertLedger(ledger), ledger);
      //make sure the hash of the
      //transactions is accurate to the known result
      try {
        hash = rippleAPI.computeLedgerHash(convertLedger(ledger));

      } catch(e) {
        log.error('hash calc error:', ledger.ledger_index, e.stack || e);
        if (startIndex) {
          process.exit(1);

        } else {
          working = false;
          return;
        }
      }

      //re-import the ledger
      //and send a notification
      if (hash !== ledger.ledger_hash) {
        message = 'ledger does not hash to the expected value ' +
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'actual ledger hash: ' + ledger.ledger_hash + '\n' +
          'calculated ledger hash:   ' + hash;
        log.error(message);

        if (startIndex) {
          //importLedger(lastValid.ledger_index + 1);
          process.exit(1);

        } else {
          notify(ledger.ledger_index, message);
          working = false;
          return;
        }


      //hash chain is broken
      //send notification
      } else if (lastValid.ledger_hash && lastValid.ledger_hash != ledger.parent_hash) {
        message = 'incorrect parent_hash:\n' +
          'ledger_index: ' + ledger.ledger_index + '\n' +
          'ledger_hash: ' + ledger.ledger_hash + '\n' +
          'parent_hash: ' + ledger.parent_hash + '\n' +
          'expected: ' + lastValid.ledger_hash;

        log.error(message);
        notify(ledger.ledger_index, message);
        working = false;
        return;

        /*
        if (lastValid.ledger_index - 1 > GENESIS_LEDGER) {
          lastValid.ledger_index--;
          lastValid.parent_hash = null;
          lastValid.ledger_hash = null;

          //wait 30 seconds then try again
          //maybe I should remove the ledger here
          setTimeout(checkNextLedger, 30000);
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
        setImmediate(checkNextLedger);

      } else {
        log.info('reached max:', max);
        process.exit(0);
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
        setImmediate(checkNextLedger);

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
    importer.getLedger({
      ledgerVersion: ledger_index
    }, function (err, ledger) {

      if (err) {
        log.error(err);

        if (startIndex) {
          process.exit(1);

        //client handler is serving an
        //unvalidated ledger, send notification
        } else if (err.indexOf('unable to validate ledger') !== -1) {
          notify(ledger_index, err);
          working = false;
          return;

        } else {
          working = false;
          return;
        }
      }

      log.info('got ledger:', ledger.ledger_index);
      self.emit('ledger', ledger, function(err, resp) {

        if (err) {
          log.error(err);
          if (startIndex) {
            process.exit(1);
          } else {
            working = false;
            return;
          }
        }

        setImmediate(checkNextLedger);
      });
    });
  }

  /**
   * notify
   * notify via email
   */

  function notify(ledger_index, message) {

    //check for previous notification
    if (!notifications[ledger_index]) {

      message = message.replace(/\n/g, '<br />\n');

      var params = {
        from: 'Storm Import Validator <storm-import-validator@ripple.com>',
        to: to,
        subject: 'Validation error',
        html: "The validation process encountered an unexpected error: <br /><br />\n" +
          '<blockquote>' + message + '</blockquote><br />\n' +
          'https://data-staging.ripple.com/v2/ledgers/' + ledger_index + ' <br />\n' +
          'https://ripple.com/build/ripple-info-tool/#' + ledger_index
      };

      transporter.sendMail(params, function(err, info) {
        if (err) {
          log.error(err);
        } else {
          log.info('Notification sent: ', ledger_index, info.accepted);
          notifications[ledger_index] = true;
        }
      });
    }
  }

  function convertLedger(ledger) {

    return {
      stateHash: ledger.account_hash,
      closeTime: moment.unix(ledger.close_time)
        .utc().format('YYYY-MM-DDTHH:mm:ss.SSS[Z]'),
      closeTimeResolution: ledger.close_time_resolution,
      closeFlags: ledger.close_flags || '',
      ledgerHash: ledger.hash || ledger.ledger_hash,
      ledgerVersion: parseInt(ledger.ledger_index || ledger.seqNum, 10),
      parentLedgerHash: ledger.parent_hash,
      parentCloseTime: moment.unix(ledger.parent_close_time)
        .utc().format('YYYY-MM-DDTHH:mm:ss.SSS[Z]'),
      totalDrops: ledger.total_coins || ledger.totalCoins,
      transactionHash: ledger.transaction_hash,
      rawTransactions: JSON.stringify(ledger.transactions)
    };
  }
};

Validator.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = Validator;
