var config = require('../config');
var rippleAPI = require('./rippleApi');
var events = require('events');
var winston = require('winston');
var Logger = require('./logger');
var moment = require('moment');

var GENESIS_LEDGER = config.get('genesis_ledger') || 1;
var TIMEOUT = 30 * 1000;
var EPOCH_OFFSET = 946684800;
/**
 * Importer
 */

var Importer = function() {
  var self = this;
  var log = new Logger({
    scope: 'importer',
    level: config.get('logLevel') || 3,
    file: config.get('logFile')
  });

  //separate hash errors from main log
  var hashErrorLog = new (require('winston').Logger)({
    transports: [
      new (winston.transports.Console)(),
      new (winston.transports.File)({ filename: './hashErrors.log' })
    ]
  });

  this.api = rippleAPI;
  this.api.connect();

  /**
   * backFill
   * begin a new backfilling thread
   */

  self.backFill = function (stopIndex, startIndex, callback) {
    var bf = new BackFiller(stopIndex, startIndex, callback);
  };

  /**
   * liveStream
   * begin a live streaming thread
   */

  self.liveStream = function () {
    if (this.stream) {
      this._active = true;
      return this.stream;

    } else {
      this.stream = new LiveStream();
    }


    return this.stream;
  };

  /**
   * stop
   * stop live stream
   */

  self.stop = function () {
    if (this._active) {
      this._active = false;
    }
  };

 /**
  * BackFiller
  * back fill the history with validated ledgers
  * from a specific starting point or latest
  * validated ledger to a specified end point
  * or the effective genesis ledger
  */
  var BackFiller = function (stopIndex, startIndex, callback) {

    var queue = {};
    var earliest;
    var earliestParentHash;

    if (stopIndex < GENESIS_LEDGER) {
      stopIndex = GENESIS_LEDGER;
    }

    if (startIndex < GENESIS_LEDGER) {
      log.info('start index precedes genesis ledger (' + GENESIS_LEDGER + ')');
      if (typeof callback === 'function') callback();
      return;
    }

    if (startIndex < stopIndex) {
      log.info('start index precedes stop index', stopIndex, startIndex);
      if (typeof callback === 'function') callback();
      return;
    }

    rippleAPI.connect().then(function() {
      getLedger(startIndex + 1);
    });

   /**
    * getLedger
    * get a specific ledger from rippled
    * if multiple ledgers are being retreived
    * simultaneously, add a little padding
    * between requests
    */
    function getLedger(index, count) {

      var options = {
        ledgerVersion: index
      };

      if (!count) count = 0;

      setTimeout(function() {
        self.getLedger(options, function (err, ledger) {
          if (ledger) {
            setImmediate(handleLedger, ledger);

          } else {
            log.error(err);
            callback('import failure', err);
          }
        });
      }, count*100);
    }

   /**
    * handleLedger
    * process the ledger returned from rippled
    */
    function handleLedger (ledger) {
      var current = Number(ledger.ledger_index);

      //if this is the first ledger,
      //we will not add it to the queue because
      //we are just getting the parent hash
      //for validation
      if (!earliest) {
        earliest = current;
        earliestParentHash = ledger.parent_hash;

      //add it to the queue
      } else {
        queue[current] = ledger;

        //move the que forward if possible
        advanceQueue();

        if (earliest === stopIndex) {
          log.info('backfill complete:', stopIndex, '-', startIndex);
          if (typeof callback === 'function') callback();
        }
      }

      //get more ledgers if there is room
      //if the queue has available space
      updateQueue();
    }

   /**
    * updateQueue
    * update the queue with new ledger
    * requests if there is any free space
    */
    function updateQueue () {
      var max    = 20;
      var num    = earliest - stopIndex;
      var length = Object.keys(queue).length;
      var count  = 0;

      if (length >= max)  num = 0;
      else if (num > max) num = max;

      for (var i=0; i < num; i++) {

        var index = earliest - i - 1;

        if (index < stopIndex) {
          break;
        }

        if (!queue[index]) {
          queue[index] = 'pending';
          getLedger(index, count++);
        }
      }
    }

   /**
    * advanceQueue
    * remove as many validated ledgers
    * from the queue as possible
    */
    function advanceQueue () {
      //move the queue if possible
      var index = earliest - 1;
      while (1) {

        if (queue[index] === 'pending') {
          break;

        } else if (queue[index] === 'failed') {
          log.warn('retry failed ledger:', index);
          getLedger(index);
          break;

        } else if (queue[index]) {
          if (earliestParentHash && earliestParentHash != queue[index].ledger_hash) {
            log.error("expected different parent hash:", index);
            callback("Unable to complete backfill: parent hash mismatch");
            break;

          } else if (earliest != index + 1) {
            log.error("unexpected index:", index);
            callback("Unable to complete backfill: unexpected index");
            break;
          }

          earliest           = index;
          earliestParentHash = queue[index].parent_hash;

          self.emit('ledger', queue[index]);
          delete queue[index];
          index--;

        } else {
          break;
        }
      }
    }
  };


 /**
  * LiveStream
  * importer class that tracks last
  * ledger closed to import in real time
  */
  var LiveStream = function () {
    var latest; //latest ledger from rippled
    var first;  //first ledger from rippled
    self._active = true;


    log.info("import: starting live stream");
    rippleAPI.connect();
    rippleAPI.on('ledger', function(resp) {
      if (!self._active) return;
      log.info('['+new Date().toISOString()+']', 'ledger closed:', resp.ledgerVersion);
      getValidatedLedger(resp.ledgerVersion);
    });

    function getValidatedLedger (index) {
      var options = {
        ledgerVersion: index,
        includeAllData: true,
        includeTransactions: true
      };

      self.getLedger(options, function (err, ledger) {
        if (ledger) {
          handleLedger(ledger);

        } else if (err) {
          log.error(err);
        }
      });
    }

    function handleLedger(ledger) {

      var current = ledger.ledgerVersion;

      // first to come in
      if (!first) {
        first  = current;
        latest = current;
      }

      // check for gap
      if (current > latest + 1) {
        log.info("starting backfill:", latest + 1, '-', current - 1);
        self.backFill(latest + 1, current - 1);
      }

      self.emit('ledger', ledger);
      latest = current;
    }
  };

  /**
   * getLedger
   * @param {Object} options
   * @param {Object} callback
   */
  self.getLedger = function (options, callback) {

    if (!options) options = {};

    var attempts = options.attempts || 0;

    delete options.attempts;
    options.includeAllData = true;
    options.includeTransactions = true;


    if (rippleAPI.isConnected()) {
      requestLedger(options, callback);

    } else {
      rippleAPI.connect().then(function() {
        requestLedger(options, callback);
      });
    }

    /**
     * requestLedger
     */

    function requestLedger(options, callback) {

      log.info('['+new Date().toISOString()+']', 'requesting ledger:', options.ledgerVersion);

      rippleAPI.getLedger(options)
      .then(processLedger)
      .catch(function(e) {
        log.error("error requesting ledger:", options.ledgerVersion, e);
        setImmediate(retry, options, attempts, callback);
      });

      /**
       * handleResponse
       */

      function processLedger(ledger) {
        var hash;

        //if we didn't request transactions,
        //we can't calculate the transactions hash
        if (!options.includeAllData || !options.includeTransactions) {
          callback(null, convertLedger(ledger));
          return;
        }


        // check hash but dont require
        try {
          hash = rippleAPI.computeLedgerHash(ledger);

        } catch (err) {
          log.error("Error calculating ledger hash: ", ledger.ledgerVersion, err);
          hashErrorLog.error(ledger.ledgerVersion, err.toString());
          //callback('unable to validate ledger: ' + ledger.ledgerVersion);
          //return;
        }

        // check but dont require
        if (hash !== ledger.ledgerHash) {
          hashErrorLog.error('hash does not match:',
                             hash,
                             ledger.ledgerHash,
                             ledger.ledgerVersion);
          //callback('unable to validate ledger: ' + ledger.ledgerVersion);
          //return;
        }

        log.info('['+new Date().toISOString()+']', 'Got ledger: ' + ledger.ledgerVersion);
        callback(null, convertLedger(ledger));
      }
    }
  };

  function convertLedger(ledger) {
    var converted = {
      account_hash: ledger.stateHash,
      close_time: moment.utc(ledger.closeTime).unix(),
      close_time_human: moment.utc(ledger.closeTime).format('YYYY-MMM-DD hh:mm:ss'),
      close_time_resolution: ledger.closeTimeResolution,
      close_flags: ledger.closeFlags,
      hash: ledger.ledgerHash,
      ledger_hash: ledger.ledgerHash,
      ledger_index: ledger.ledgerVersion.toString(),
      seqNum: ledger.ledgerVersion.toString(),
      parent_hash: ledger.parentLedgerHash,
      parent_close_time: moment.utc(ledger.parentCloseTime).unix(),
      total_coins: ledger.totalDrops,
      totalCoins: ledger.totalDrops,
      transaction_hash: ledger.transactionHash,
      transactions: []
    };

    if (ledger.rawTransactions) {
      converted.transactions = JSON.parse(ledger.rawTransactions);
    }

    return converted;
  }

 /**
  * retry
  * @param {Object} ledgerIndex
  * @param {Object} attempts
  * @param {Object} callback
  */

  function retry(options, attempts, callback) {
    if (attempts >= 10) {
      log.error('failed to get ledger after ' + attempts + ' attempts:', options.ledgerVersion);
      callback("failed to get ledger");
      return;
    }

    options.attempts = attempts + 1;
    log.info("retry attempts:", options.attempts);

    setTimeout(function(opts, att, cb) {
      self.getLedger(options, callback);
    }, 250);
  }

  return this;
};

Importer.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = new Importer();

/*
function sizeof(normal_val) {
  // Force string type
  normal_val = JSON.stringify(normal_val);

  var byteLen = 0;
  for (var i = 0; i < normal_val.length; i++) {
    var c = normal_val.charCodeAt(i);
    byteLen += c < (1 <<  7) ? 1 :
               c < (1 << 11) ? 2 :
               c < (1 << 16) ? 3 :
               c < (1 << 21) ? 4 :
               c < (1 << 26) ? 5 :
               c < (1 << 31) ? 6 : Number.NaN;
  }
  return byteLen / 1000;
}
*/

