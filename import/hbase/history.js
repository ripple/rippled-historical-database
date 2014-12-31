var config   = require('../../config/import.config');
var log      = require('../../lib/log')('hbase_history');
var moment   = require('moment');
var Importer = require('../importer');
var DB       = require('./client');
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var HistoricalImport = function () {
  this.importer = new Importer();
  this.count    = 0;
  this.total    = 0;
  this.section  = { };
  this.db       = new DB();
  var self = this;
  var stopIndex;
  var cb;

  
 /**
  * handle ledgers from the importer
  */  
  
  this.importer.on('ledger', function(ledger) {
    self.db.saveLedger(ledger, function(err, resp) {
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
          
          if (self.section.error) {
            log.info("Error in section - retrying:", self.section.stopIndex, '-', self.section.startIndex);  
            self._findGaps(self.section.startIndex, null, stopIndex);
            
          } else {
            log.info("gap filled:", self.section.startIndex, '-', self.section.stopIndex);
            if (self.section.stopIndex === stopIndex) {
              log.info("stop index reached: ", stopIndex); 
              if (cb) cb();
              return; 
            }
            
            self._findGaps(self.section.stopIndex + 1, stopIndex);
          }
        }      
      }
    });
  });
  
  
  this.start = function (start, stop, callback) {
    var self  = this;
    stopIndex = stop;
    cb        = callback;
    
    if (!start || start < GENESIS_LEDGER) {
      start = GENESIS_LEDGER;
    }
    
    log.info("starting historical import: ", start, stop);
    
    if (stop && stop !== 'validated') {
      self._findGaps(start, stop); 
    
    //get latest validated ledger as the 
    //stop point for historical importing        
    } else {
      self._getLedgerRecursive('validated', 0, function(err, ledger) {
        if (err) {
          log.error('failed to get latest validated ledger');
          callback('failed to get latest validated ledger');
          return;
        } 
        
        stopIndex = parseInt(ledger.ledger_index, 10) - 1;
        self._findGaps(start, stopIndex);
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
  

  this._findGaps = function (start, stop) {
    log.info("finding gaps from ledgers:", start, stop); 
    var self = this;
    
    this._findGap({
      index      : start,
      start      : start,
      stop       : stop
    }, function(err, resp) { 
      if (err) {
        log.error(err);
        
      } else if (resp) {
        self.importer.backFill(resp.startIndex, resp.stopIndex);
        self.count   = 0;
        self.total   = resp.stopIndex - resp.startIndex + 1;
        self.section = resp;
      }
    });
  };
  
  this._findGap = function (params, callback) {
    var self = this;
    var end        = params.index + 200; 
    var startIndex = params.index;
    var stopIndex  = end;
    var ledgerHash = params.ledger_hash;
    
    if (params.stop && end > params.stop) {
      end = params.stop;
    }
    
    log.info('validating ledgers:', startIndex, '-', end);
    
    self.db.getLedgers({
      startIndex : startIndex,
      stopIndex  : end
    }, function (err, ledgers) {
      
      if (err) {
        callback(err);
        return;
      }

      if (!ledgers.length) {
        log.info('missing ledger at:', startIndex);
        callback(null, {startIndex:startIndex, stopIndex:end});
        return;
      }
      
      for (var i=0; i<ledgers.length; i++) {
        if (ledgers[i].ledger_index !== startIndex) {
          log.info('missing ledger at:', startIndex);
          log.info("gap ends at:", ledgers[i].ledger_index);
          callback(null, {startIndex:startIndex, stopIndex:ledgers[i].ledger_index});
          return;
        
        } if (ledgerHash && ledgerHash !== ledgers[i].parent_hash) {
          log.info('incorrect ledger hash at:', startIndex);
          callback(null, {startIndex:startIndex, stopIndex:startIndex});
          return;         
        }
        
        ledgerHash = ledgers[i].ledger_hash;
        startIndex++;
      }
      
           
      if (startIndex !== end || end < params.stop) {
        self._findGap({
          index : startIndex,
          stop  : params.stop
        }, callback);
        
      } else {
        log.info("stop index reached: ", end, params.stop); 
        callback(null, null);
        if (cb) cb();
        return; 
      }
    });
  };
};

module.exports = HistoricalImport;
  