var config  = require('./config.json');
var ripple  = require('ripple-lib');
var Ledger  = require('../node_modules/ripple-lib/src/js/ripple/ledger').Ledger;
var winston = require('winston');
var events  = require('events');
var emitter = new events.EventEmitter();
var options = {
  trace   : false,
  trusted : false,
  servers : config.servers,
  allow_partial_history : false,
};

var LedgerStream = function () {
  var self   = this;
  var remote = new ripple.Remote(options);
  
  remote.on('connect', function() {
    winston.info("connected");
  });

  remote.on('disconnect', function() {
    winston.info("disconnected");
  }); 
      
  /**
   * backFill
   * begin a new backfilling thread
   */
  self.backFill = function (stopIndex, startIndex) {
    var bf = new BackFiller(stopIndex, startIndex, function(){
      delete bf;
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
    
    //set the start and stop index depending
    //on what was specified
    if (!stopIndex) {
      stopIndex  = 32570;  
    } else if (!startIndex) {
      startIndex = stopIndex;
      stopIndex  = 32570; 
    } else {
      startIndex++;
    }
    
    if (remote.isConnected()) {
      getLedger(startIndex);
            
    } else {
      remote.connect();
      remote.once('connected', function(){
        getLedger(startIndex); 
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
      setTimeout(function() {
        self.getLedger({index:index || 'validated'}, function (err, ledger) {
          if (ledger) handleLedger(ledger);  
          else if (index) {
            queue[index] = 'failed';
          } else {
            console.log("failed to get validated ledger");
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

      //if this is the start ledger,
      //we will not add it to the queue
      if (!earliest) {
        
        //make sure we have the start index
        if (!startIndex) {
          startIndex = current; 
        } 
        
        earliest           = current;
        earliestParentHash = ledger.parent_hash;
        
      //add it to the queue    
      } else {
        queue[current] = ledger;
      }

      //move the que forward if possible
      advanceQueue(); 
      
      //get more ledgers if there is room 
      //if the queue has available space
      updateQueue();  
      
      if (earliest === stopIndex) {
        winston.info('backfill complete:', stopIndex, '-', startIndex);
        if (typeof callback === 'function') callback(); 
      }
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
      
      if (length >= max)  num = 0;
      else if (num > max) num = max;
      
      for (var i=0; i < num; i++) {

        var index = earliest - i - 1;
        
        if (index < stopIndex) {
          break;
        }
        
        if (!queue[index]) {
          queue[index] = 'pending';
          getLedger(index, i);
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
          winston.warn('retry failed ledger:', index);
          getLedger(index);
          break; 
        
        } else if (queue[index]) {
          if (earliestParentHash && earliestParentHash != queue[index].ledger_hash) {
            winston.error("expected different parent hash:", index);
            callback("Unable to complete backfill: parent hash mismatch");
            break;
            
          } else if (earliest != index + 1) {
            winston.error("unexpected index:", index);
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
    
    remote.connect();
    
    remote.on('ledger_closed', function(resp, server) {
      winston.info('['+new Date().toISOString()+']', 'ledger closed:', resp.ledger_index); 
      var options = {
        index    : 'validated',
        server   : server,
      };
      
      self.getLedger(options, function (err, ledger) {
        if (ledger) handleLedger(ledger);  
      });
    });

    function handleLedger(ledger) {
   
      var current = parseInt(ledger.ledger_index, 10);

      //first to come in
      if (!first) {
        first  = current;
        latest = current;
      
      //this can happen when validated returns the same
      //ledger we got last time
      } else if (latest === current) {
        winston.warn("already imported this ledger:", current);
        return; 
      } 
      
      
      //there is a gap that needs to be filled
      if (current > latest + 1) {
        winston.info("starting backfill:", latest + 1, '-', current - 1);
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
    var params = {
      transactions : true, 
      expand       : true,
    }
    
    if (isNaN(options.index) && options.index !== 'validated') {
      winston.error("invalid ledger index");
      callback("invalid ledger index");
      return;  
    }
    
    var attempts = options.attempts || 0;
    
    try {
      var request = remote.request_ledger(options.index, params, handleResponse).timeout(5000, function(){
        winston.warn("ledger request timed out after 5 seconds:", options.index);
        retry(options.index, attempts, callback); 
      });
      
      if (options.server) {
        request.setServer(options.server);
      }
      
      var info    = request.server ? request.server._url + ' ' + request.server._pubkey_node : '';
      winston.info('['+new Date().toISOString()+']', 'requesting ledger:', options.index, info);   
          
    } catch (e) {
      winston.error("error requesting ledger:", options.index, e); 
      callback("error requesting ledger");
      return;
    }
    
    function handleResponse (err, resp) {
      if (err || !resp || !resp.ledger) {
        winston.error("error:", err); 
        retry(options.index, attempts, callback); 
        return;
      }    
      
      if (!isValid(resp.ledger)) {
        retry(options.index, attempts, callback); 
        return;  
      }
    
      winston.info('['+new Date().toISOString()+']', 'Got ledger: ' + resp.ledger.ledger_index);   
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
      winston.info('ledger not closed:', ledger.ledger_index);
      return false;     
    }
    
    try {
     txHash = Ledger.from_json(ledger).calc_tx_hash().to_hex();
    } catch(err) {
      winston.error("Error calculating transaction hash: "+ledger.ledger_index +" "+ err);
      txHash = '';
    } 
    
    if (!txHash || txHash !== ledger.transaction_hash) {
      winston.info('transactions do not hash to the expected value for ' + 
        'ledger_index: ' + ledger.ledger_index + '\n' +
        'ledger_hash: ' + ledger.ledger_hash + '\n' +
        'actual transaction_hash:   ' + txHash + '\n' +
        'expected transaction_hash: ' + ledger.transaction_hash);
        
      retry(ledger.ledger_index, attempts, callback); 
      return false;
    } 
    
    for (var i=0; i<ledger.transactions.length;i++) {
      if(!ledger.transactions[i].metaData) {
        winston.info('transaction in ledger: ' + ledger.ledger_index + ' does not have metaData');
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
      winston.error('failed to get ledger after ' + attempts + ' attempts:', ledgerIndex);      
      callback("failed to get ledger");
      return;  
    }
    
    attempts++;
    winston.info("retry attempts:", attempts);
    setTimeout(function() {
      self.getLedger({index:ledgerIndex, attempts:attempts}, callback);            
    }, 250);
  }
};

LedgerStream.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = LedgerStream;



