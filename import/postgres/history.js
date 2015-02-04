var config   = require('../../config/import.config');
var log      = require('../../lib/log')('postgres_history');
var moment   = require('moment');
var diff     = require('deep-diff');
var Importer = require('../importer');
var db       = require('./client');
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

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
      log.info('ledger saved:', ledger.ledger_index, '---', self.count, 'of', self.total);
      if (err) {
        self.section.error = true;
        
      } else if (resp) {  
        if (resp.ledger_index === self.section.stopIndex) {
          self.section.stopHash = resp.ledger_hash;
        }
      }
        
      if (self.count === self.total) {
        self._findGaps(self.section.stopIndex, self.section.stopHash, stopIndex);
      /*
      if (self.section.error) {
        log.info("Error in section - retrying:", self.section.stopIndex, '-', self.section.startIndex);  
        self._findGaps(self.section.startIndex, null);

      } else {
        store.setItem('earliestValidated', {index:self.section.stopIndex, hash:self.section.stopHash});
        log.info("gap filled:", self.section.stopIndex, '-', self.section.startIndex);
        self._findGaps(self.section.stopIndex, self.section.stopHash);
      }
      */
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
        var hash  = ledger.parent_hash.toLowerCase();
        
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
  

  this._findGaps = function (start, startParentHash, stop) {
    log.info("finding gaps - start: ", start, stop ? ' stop: ' + stop : ''); 
    var self = this;
    
    this._findGap({
      validated  : start,
      parentHash : startParentHash,
      stop       : stop
      
    }, function(err, resp) {

      if (resp) {
        if (resp.startIndex < GENESIS_LEDGER) {
          log.info("Genesis ledger reached:", GENESIS_LEDGER);  
          if (cb) cb();
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
    var self       = this;
    var end        = params.validated - 50;
    var ledgerHash = params.parentHash; 
    var index      = params.validated;
    var check;
    var startIndex;
    
    if (params.stop && end < params.stop) {
      end = params.stop;
    }
    
    if (params.start && params.start - params.validated >= 200) {
      log.info("max gap size reached:", params.start);
      callback(null, {startIndex:params.start, stopIndex:params.start - 200}); 
      return;   
    }
    
    log.info('validating ledgers:', end, '-', params.validated);
    db.getLedgers({startIndex:end, stopIndex:params.validated}, function(err, ledgers) {
      if (err) {
        callback(err);
        return;
      }
      
      //advance to the next batch
      if (!ledgers.length) {
        if (params.stop && params.stop === end) {
          log.info("stop index reached: ", params.stop);
          callback(null, {startIndex:params.start || params.validated, stopIndex:end});
          return;
        }
        
        self._findGap({
          validated  : end,
          parentHash : ledgerHash, 
          start      : params.start || params.validated, 
          stop       : params.stop
        }, callback); 
        return;
      }
      
      
      for (var i=0; i<ledgers.length; i++) {  
        check = parseInt(ledgers[i].ledger_index, 10);
        
        if (params.start) {
          log.info("gap ends at:", check);
          callback(null, {startIndex:params.start, stopIndex:check});
          return;

        } else if (check < index) {
          log.info("missing ledger at:", index); 
          log.info("gap ends at:", check);
          callback(null, {startIndex:index, stopIndex:check});
          return;
          
        } else if (check > index) {
          log.info("duplicate ledger index:", check); 
          callback();
          if (cb) cb();
          return;
          
        } else if (ledgerHash && ledgerHash !== ledgers[i].ledger_hash) {
          log.info("incorrect ledger hash at:", check); 
          callback(null, {startIndex:check, stopIndex:check});
          return;

        } else if (params.stop && check<=params.stop) {
          log.info("stop index reached: ", check); 
          callback(null, null);
          if (cb) cb();
          return;        
          
        } else {
          ledgerHash = ledgers[i].parent_hash;
          index = check - 1;
        }
      }    
      
      if (index > end) {
        startIndex = index;
        end        = index;
        ledgerHash = null;
        log.info("missing ledger at:", index); 
        
      } else {
        end        = ledgers[ledgers.length-1].ledger_index;
        ledgerHash = ledgers[ledgers.length-1].ledger_hash;
      }
      
      self._findGap({
        validated  : end,
        parentHash : ledgerHash,
        start      : startIndex,
        stop       : params.stop
      }, callback); 
    });  
  };
}

module.exports = HistoricalImport;
