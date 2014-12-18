var config   = require('../../config/import.config');
var log      = require('../../lib/log')('couchdb_history');
var moment   = require('moment');
var diff     = require('deep-diff');
var ripple   = require('ripple-lib');
var Importer = require('../importer');
var store    = require('node-persist');
var indexer  = require('./indexer');
var db       = require('./client');
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

store.initSync();

var HistoricalImport = function () {
  this.importer = new Importer();
  this.count    = 0;
  this.total    = 0;
  this.section  = { };
  var self = this;
  var stopIndex;
  var cb;
  
 /**
  * handle ledgers from the importer
  */  
  this.importer.on('ledger', function(ledger) {
    db.saveLedger(ledger, function(err, resp) {
      self.count++;
      if (err) {
        log.error(err);
        self.section.error = true;
        
     } else if (resp) {  
        log.info(self.count, 'of', self.total);
        if (resp.ledger_index === self.section.stopIndex) {
          self.section.stopHash = resp.ledger_hash;
        }
        
        if (self.count === self.total) {
          indexer.pingCouchDB();
          
          if (self.section.error) {
            log.info("Error in section - retrying:", self.section.stopIndex, '-', self.section.startIndex);  
            self._findGaps(self.section.startIndex, null, stopIndex);
            
          } else {
            store.setItem('earliestValidated', {index:self.section.stopIndex, hash:self.section.stopHash});
            log.info("gap filled:", self.section.stopIndex, '-', self.section.startIndex);
            self._findGaps(self.section.stopIndex, self.section.stopHash, stopIndex);
          }
        }      
      }
    });
  });
  
  this.start = function (start, stop, callback) {
    log.info("starting historical import: ", start, stop);
    stopIndex = stop;
    cb        = callback;
    
    if (start && start !== 'validated') {
      self._findGaps(start, null, stop); 
    
    //get latest validated ledger as the 
    //starting point for historical importing        
    } else {
      self._getLedgerRecursive('validated', 0, function(err, ledger) {
        if (err) {
          log.error('failed to get latest validated ledger');
          callback('failed to get latest validated ledger');
          return;
        } 
        
        var index = parseInt(ledger.ledger_index, 10) - 1;
        var hash  = ledger.parent_hash.toUpperCase();
        
        self._findGaps(index, hash, stop);
      });      
    }    
  };

  this._getLedgerRecursive = function(index, attempts, callback) {
    var self = this;
    
    if (attempts && attempts > 10) {
      callback("failed to get ledger");
      return;
    }
    
    self.importer.getLedger({index:index}, function(err, ledger) {
      if (err) {
        log.error(err, "retrying");
        self._getLedgerRecursive(index, ++attempts, callback);
        return;
      } 
      
      callback(null, ledger);
    });  
  };
  

  this._findGaps = function (start, parentHash, stop) {
    log.info("finding gaps from ledger:", start); 
    var self = this;
    
    this._findGap({
      index      : start,
      parentHash : parentHash,
      stop       : stop
    }, function(err, resp) {
      if (resp) {  
        if (resp.startIndex < GENESIS_LEDGER) {
          log.info("Genesis ledger reached:", GENESIS_LEDGER);  
          return;
        } else if (resp.stopIndex < GENESIS_LEDGER) {
          log.info("setting stop index to genesis ledger");
          resp.stopIndex = GENESIS_LEDGER;
        }
        
        self.importer.backFill(resp.stopIndex, resp.startIndex);
        self.count   = 0;
        self.total   = resp.startIndex - resp.stopIndex + 1;
        self.section = resp;
      }
    });
  };

  this._findGap = function (params, callback) {
    var self = this;
    var end  = params.index - 50;
    var ids  = []; 
    var ledgerHash = params.parentHash;    
    var startIndex;
    var check;
    
    if (params.stop && end < params.stop) {
      end = params.stop;
    }
    
    if (params.start && params.start - params.index >= 200) {
      log.info("max gap size reached:", params.start);
      callback(null, {startIndex:params.start, stopIndex:params.start - 200}); 
      return;   
    } 
    
    for (var i = params.index; i >= end; i--) {
      ids.push(db.addLeadingZeros(i));
    }
    
    log.info('validating ledgers:', end, '-', params.index);
    db.nano.fetch({keys:ids}, function(err, resp){
      
      if (err || !resp.rows || !resp.rows.length) {
        if (!err) console.log(resp);
        callback(err || 'invalid response from couchdb');
        return;
      }
      
      for (var i=0; i<resp.rows.length; i++) {
        check = parseInt(resp.rows[i].key, 10);
        
        if (resp.rows[i].doc && params.start) {
          log.info("gap ends at:", check);
          callback(null, {startIndex:params.start, stopIndex:check});
          return;
          
        } else if (resp.rows[i].doc && ledgerHash && resp.rows[i].doc.ledger_hash !== ledgerHash) {
          log.info("incorrect ledger hash at:", check);
          console.log(ledgerHash);
          console.log(resp.rows[i].doc.ledger_hash);
          console.log(resp.rows[i].doc.parent_hash);
          params.start = check;
          
        } else if (!resp.rows[i].doc && !params.start) {
          log.info("missing ledger at:", check);
          params.start = check;

        } else if (params.stop && check<=params.stop) {
          log.info("stop index reached: ", check); 
          
          if (params.start) {
            if (resp.rows[i].doc) check++;  //stop at the one before
            callback(null, {startIndex:params.start, stopIndex:check});  
            
          } else { 
            callback(null, null);
            if (cb) cb();
          }
          return; 
        
        } else if (!resp.rows[i].doc) {
          ledgerHash = null;
          
        } else {
          ledgerHash = resp.rows[i].doc.parentHash;
        } 
      }
      
      
      self._findGap({
        index      : end, 
        parentHash : ledgerHash, 
        start      : params.start,
        stop       : params.stop
      }, callback);   
    });  
  };
}

module.exports = HistoricalImport;
