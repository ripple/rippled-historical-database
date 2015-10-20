var ripple  = require('ripple-lib');
var events  = require('events');
var winston = require('winston');
var Logger  = require('./logger');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger
var TIMEOUT = 30 * 1000;
var EPOCH_OFFSET = 946684800;
/**
 * Importer
 */

var Importer = function (options) {
  var self   = this;
  var remote = new ripple._DEPRECATED.Remote(options.ripple);

  var log    = new Logger({
    scope : 'importer',
    level : options.logLevel || 3,
    file  : options.logFile
  });

  //separate hash errors from main log
  var hashErrorLog = new (require('winston').Logger)({
    transports: [
      new (winston.transports.Console)(),
      new (winston.transports.File)({ filename: './hashErrors.log' })
    ]
  });

  log.level(options.logLevel || 2);
  remote.connect();

  remote.on('connect', function() {
    log.info("import: Rippled connected");
  });

  remote.on('disconnect', function() {
    log.info("import: Rippled disconnected");
  });

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

    if (remote.isConnected()) {
      getLedger(startIndex + 1);

    } else {
      remote.connect();
      remote.once('connected', function(){
        getLedger(startIndex + 1);
      });
    }

   /**
    * getLedger
    * get a specific ledger from rippled
    * if multiple ledgers are being retreived
    * simultaneously, add a little padding
    * between requests
    */
    function getLedger(index, count) {
      if (!count) count = 0;

      setTimeout(function() {
        self.getLedger({index:index || 'validated'}, function (err, ledger) {
          if (ledger) handleLedger(ledger);
          else {
            log.error(err);
            if (count++<30) {
              log.error('backfiller failed to get ledger, retrying: ' + count);
              getLedger(index, count);
            } else {
              log.error('backfiller failed to get ledger, stopping after 30 attempts');
              if (typeof callback === 'function') {
                callback('import failure');
              }
            }
          }
        });
      }, count*100);
    }

   /**
    * handleLedger
    * process the ledger returned from rippled
    */
    function handleLedger (ledger) {
      var current = parseInt(ledger.ledger_index, 10);

      //if this is the first ledger,
      //we will not add it to the queue because
      //we are just getting the parent hash
      //for validation
      if (!earliest) {

        earliest           = current;
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
    remote.connect();

    remote.on('ledger_closed', function(resp, server) {
      if (!self._active) return;

      log.info('['+new Date().toISOString()+']', 'ledger closed:', resp.ledger_index);
      getValidatedLedger(resp.ledger_index, server);
    });



    function getValidatedLedger (index, server) {
      var options = {
        index    : 'validated',
        server   : server,
      };

      self.getLedger(options, function (err, ledger) {
        if (ledger) {
          var current = parseInt(ledger.ledger_index, 10);

          //retry if we get the previously closed ledger
          if (index && index === current + 1) {
            log.warn("ledger not most recent:", ledger.ledger_index);
            getValidatedLedger (index, server);

          } else {
            handleLedger(ledger);
          }

        } else if (err) {
          log.error(err);
        }
      });
    }

    function handleLedger(ledger) {

      var current = parseInt(ledger.ledger_index, 10);

      //first to come in
      if (!first) {
        first  = current;
        latest = current;

      //this can happen when validated returns the same
      //ledger we got last time
      } else if (latest === current) {
        log.warn("already imported this ledger:", current);
        return;
      }


      //there is a gap that needs to be filled
      if (current > latest + 1) {
        log.info("starting backfill:", latest + 1, '-', current - 1);
        self.backFill(latest + 1, current - 1);
      }

      self.emit('ledger', ledger);
      latest = current;
      return true;
    }
  };

  /**
   * getLedger
   * @param {Object} options
   * @param {Object} callback
   */
  self.getLedger = function (options, callback) {
    var attempts = options.attempts || 0;
    var params   = { }

    //default is return transactions
    if (options.transactions !== false) {
      params.transactions = true;
    }

    //default is expand transactions
    if (options.expand !== false) {
      params.expand = true;
    }

    if (options.index === 'validated') {
      params.validated = true;

    } else if (!isNaN(options.index)) {
      params.ledger_index = options.index;

    } else {
      log.error("invalid ledger index");
      callback("invalid ledger index");
      return;
    }

    if (remote.isConnected()) {
      requestLedger(params, callback);

    } else {
      remote.once('connect', function() {
        requestLedger(params, callback);
      });
    }

    /**
     * requestLedger
     */

    function requestLedger(options, callback) {
      var index = options.validated ? 'validated' : options.ledger_index;

      try {
        var request = remote.requestLedger(options, handleResponse);

        if (options.server) {
          request.setServer(options.server);
        }

        var info  = request.server ? request.server.getServerID() : '';
        log.info('['+new Date().toISOString()+']', 'requesting ledger:', index, info);

      } catch (e) {
        log.error("error requesting ledger:", index, e);
        callback("error requesting ledger");
        return;
      }

      /**
       * handleResponse
       */

      function handleResponse (err, resp) {

        if (err || !resp || !resp.ledger) {
          log.error("error:", err ? err.message || err : null);
          retry(options.ledger_index, attempts, callback);
          return;
        }

        if (!resp.validated) {
          callback('ledger not validated: ' +
                   resp.ledger.ledger_index + ' ' +
                   resp.ledger.ledger_hash);
          return;
        }

        //if we didn't request transactions,
        //we can't calculate the transactions hash
        if (!options.transactions || !options.expand) {
          callback(null, resp.ledger);
          return;
        }

        try {
          var valid = isValid(resp.ledger);

        } catch (err) {
          log.error("Error calculating transaction hash: "+resp.ledger.ledger_index +" "+ err);
          hashErrorLog.error(resp.ledger.ledger_index, err.toString());
          valid = true;
        }

        if (!valid) {
          log.error('unable to validate ledger:', resp.ledger.ledger_index);
          retry(Number(resp.ledger.ledger_index), attempts, callback);
          return;
        }

        log.info('['+new Date().toISOString()+']', 'Got ledger: ' + resp.ledger.ledger_index);
        callback(null, resp.ledger);
      }
    }
  };

 /**
  * isValid
  * @param {Object} ledger
  */
  function isValid (ledger) {
    var hash;
    var converted = convertLedger(ledger);

    if (!ledger.closed) {
      log.info('ledger not closed:', ledger.ledger_index);
      return false;
    }

    hash = ripple.RippleAPI._PRIVATE.computeLedgerHash(converted);
    return hash === ledger.hash;
  }

  function convertLedger(ledger) {
    return {
      accepted: ledger.accepted,
      closed: ledger.closed,
      stateHash: ledger.account_hash,
      closeTime: ledger.close_time,
      closeTimeResolution: ledger.close_time_resolution,
      closeFlags: ledger.close_flags || '',
      ledgerHash: ledger.hash || ledger.ledger_hash,
      ledgerVersion: parseInt(ledger.ledger_index || ledger.seqNum, 10),
      parentLedgerHash: ledger.parent_hash,
      parentCloseTime: ledger.parent_close_time || '',
      totalDrops: ledger.total_coins || ledger.totalCoins,
      transactionHash: ledger.transaction_hash,
      rawTransactions: JSON.stringify(ledger.transactions)
    };
  }

 /**
  * retry
  * @param {Object} ledgerIndex
  * @param {Object} attempts
  * @param {Object} callback
  */
  function retry(ledgerIndex, attempts, callback) {
    if (attempts >= 10) {
      log.error('failed to get ledger after ' + attempts + ' attempts:', ledgerIndex);
      callback("failed to get ledger");
      return;
    }

    attempts++;
    log.info("retry attempts:", attempts);
    setTimeout(function() {
      self.getLedger({index:ledgerIndex, attempts:attempts}, callback);
    }, 250);
  }

  return this;
};

Importer.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = Importer;

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

