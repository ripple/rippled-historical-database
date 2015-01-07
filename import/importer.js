var config  = require('../config/import.config');
var ripple  = require('ripple-lib');
var Ledger  = require('../node_modules/ripple-lib/src/js/ripple/ledger').Ledger;
var log     = require('../lib/log')('import');
var events  = require('events');
var emitter = new events.EventEmitter();
var winston = require('winston');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger
var TIMEOUT = 30 * 1000;

var hashErrorLog = new (require('winston').Logger)({
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({ filename: './hashErrors.log' })
  ]   
});

log.level(config.get('logLevel') || 2);

var Importer = function () {
  var self   = this;
  var remote = new ripple.Remote(config.get('ripple'));
  
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
    var bf = new BackFiller(stopIndex, startIndex, function() {
      delete bf;
      if (typeof callback === 'function') callback(); 
    });  
  };
  
  /**
   * liveStream
   * begin a live streaming thread
   */
  self.liveStream = function () {
    return new LiveStream();
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
            //the live importer will fail after 10
            //attempts, but the backfiller must retry indefinitely
            log.error('backfiller failed to get ledger, retrying');
            getLedger(index);
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
      var max    = config.get('queueLength');
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
    
    log.info("import: starting live stream");
    remote.connect();
    
    try {
      remote.once('connect', function(){ 
        remote.on('ledger_closed', function(resp, server) {
          log.info('['+new Date().toISOString()+']', 'ledger closed:', resp.ledger_index);       
          getValidatedLedger(resp.ledger_index, server);
        });
      });
    } catch (e) {
      console.log(e);
    }

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
    var params   = {
      transactions : true, 
      expand       : true
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
    
    function requestLedger(options, callback) {
      var index = options.validated ? 'validated' : options.ledger_index;
      
      try {
        var request = remote.request_ledger(options, handleResponse).timeout(TIMEOUT, function(){
          log.warn('ledger request timed out after ' + (TIMEOUT/1000) + ' seconds:', index);
          retry(index, attempts, callback); 
        });
        
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
    }
    
    function handleResponse (err, resp) {

      if (err || !resp || !resp.ledger) {
        log.error("error:", err ? err.message || err : null); 
        retry(options.index, attempts, callback); 
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
        retry(options.index, attempts, callback); 
        return;  
      }
    
      log.info('['+new Date().toISOString()+']', 'Got ledger: ' + resp.ledger.ledger_index);   
      callback(null, resp.ledger); 
    }
  };
  
 /**
  * isValid
  * @param {Object} ledger
  */
  function isValid (ledger) {
    var txHash;
          
    if (!ledger.closed) {
      log.info('ledger not closed:', ledger.ledger_index);
      return false;     
    }
    
    txHash = Ledger.from_json(ledger).calc_tx_hash().to_hex();
    
    if (txHash !== ledger.transaction_hash) {
      log.info('transactions do not hash to the expected value for ' + 
        'ledger_index: ' + ledger.ledger_index + '\n' +
        'ledger_hash: ' + ledger.ledger_hash + '\n' +
        'actual transaction_hash:   ' + txHash + '\n' +
        'expected transaction_hash: ' + ledger.transaction_hash);
        
      return false;
    } 
    
    for (var i=0; i<ledger.transactions.length;i++) {
      if(!ledger.transactions[i].metaData) {
        log.info('transaction in ledger: ' + ledger.ledger_index + ' does not have metaData');
        return false;
      }
    }
    
    return true;     
  }
  
 /**
  * retry
  * @param {Object} ledgerIndex
  * @param {Object} attempts
  * @param {Object} callback
  */ 
  function retry (ledgerIndex, attempts, callback) {
    
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

