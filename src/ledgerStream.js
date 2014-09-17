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
  var last   = {};
  
  self.start = function () {
    remote.connect();
    
    remote.on('ledger_closed', function(resp){
      winston.info('['+new Date().toISOString()+']', 'ledger closed:', resp.ledger_index); 
      self.getLedger(resp.ledger_index);
    });
  
    remote.on('connect', function() {
      winston.info("connected");
    });
  
    remote.on('disconnect', function() {
      winston.info("disconnected");
    }); 
  };

  /**
   * 
   * @param {Object} ledgerIndex
   * @param {Object} callback
   */
  self.getLedger = function (ledgerIndex, attempts, callback) {
    var options = {
      transactions:true, 
      expand:true,
    }
    
    if (isNaN(ledgerIndex)) {
      if (typeof callback === 'function') callback("invalid ledger index");
      winston.error("error:", "invalid ledger index");
      return;  
    }
    
    if (!attempts) {
      attempts = 0;
    }
    
    var request = remote.request_ledger(ledgerIndex, options, function(err, resp) {
      var ledgerIndex = this.message.ledger;
      if (err || !resp || !resp.ledger) {
        winston.error("error:", err); 
        retry(ledgerIndex, attempts, callback); 
        return;
      }    
      
      self.handleLedger (ledgerIndex, resp.ledger, attempts, callback);
    });
    
    var info = request.server ? request.server._url + ' ' + request.server._pubkey_node : '';
    winston.info('['+new Date().toISOString()+']', 'requesting ledger:', ledgerIndex, info);   
  };
  
  self.handleLedger = function (ledgerIndex, ledger, attempts, callback) {
    var txHash;
          
    if (!ledger.closed) {
      winston.info('ledger not closed:', ledgerIndex);
      retry(ledgerIndex, attempts, callback); 
      return;     
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
        
      retry(ledgerIndex, attempts, callback); 
      return;
    } 

    winston.info('['+new Date().toISOString()+']', 'Got ledger: ' + ledger.ledger_index);   
    if (typeof callback === 'function') callback(null, ledger);
    else self.emit('ledger', ledger);   
  };
  
  function retry (ledgerIndex, attempts, callback) {
    
    if (attempts >= 20) {
      if (typeof callback === 'function') callback("failed to get ledger");
      winston.error('failed to get ledger after ' + attempts + ' attempts:', ledgerIndex);
      return;  
    }
    
    attempts++;
    winston.info("retry attempts:", attempts);
    setTimeout(function() {
      self.getLedger(ledgerIndex, attempts, callback);            
    }, 250);
  }
};

LedgerStream.prototype.__proto__ = events.EventEmitter.prototype;

module.exports = LedgerStream;



