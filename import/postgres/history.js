var config   = require('../../config/import.config');
var log      = require('../../lib/log')('postgres_history');
var moment   = require('moment');
var diff     = require('deep-diff');
var ripple   = require('ripple-lib');
var Importer = require('../importer');
var db       = require('./client')(config.get('sql'));
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

var HistoricalImport = function () {
  this.importer = new Importer();
  this.count    = 0;
  this.total    = 0;
  this.section  = { };
  var self = this;
  var first;
  
 /**
  * handle ledgers from the importer
  */  
  this.importer.on('ledger', function(ledger) {
    db.saveLedger(ledger, function(err, resp) {
      self.count++;
      log.info(self.count, 'of', self.total);
      if (err) {
        self.section.error = true;
        
      } else if (resp) {  
        if (resp.ledger_index === self.section.stopIndex) {
          self.section.stopHash = resp.ledger_hash;
        }
      }
        
      if (self.count === self.total) {
        self._findGaps(self.section.stopIndex, self.section.stopHash);
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
  
  this.start = function () {
    var start = config.get('startIndex');
    if (start === 'validated') {
      first = null;
    } else if (start) {
      first = {index:start};
    } else {
      //get from db control table
      console.log("get from control");
      return;
    }
    
    console.log(first);
    
    if (first) {
      self._findGaps(first.index, first.hash); 
       
    } else {
      
      //get latest validated ledger as the 
      //starting point for historical importing
      self._getLedgerRecursive('validated', 0, function(err, ledger) {
        if (err) {
          log.error("failed to get latest validated ledger");
          return;
        } 
        
        self._findGaps(ledger.ledger_index, ledger.ledger_hash.toLowerCase());
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
  

  this._findGaps = function (start, parentHash) {
    log.info("finding gaps from ledger:", start); 
    var self = this;
    
    this._findGap(start, parentHash, null, function(err, resp) {
      console.log(err, resp);
      if (resp) {
        self.importer.backFill(resp.stopIndex, resp.startIndex);
        self.count   = 0;
        self.total   = resp.startIndex - resp.stopIndex + 1;
        self.section = resp;
      }
    });
  };

  this._findGap = function (validated, ledgerHash, startIndex, callback) {
    var self = this;
    var end  = validated - 50;
    var ids  = [];
    var stopIndex; 
  
    if (startIndex && startIndex - validated > 200) {
      log.info("max gap size reached:", startIndex);
      callback(null, {startIndex:startIndex, stopIndex:startIndex - 200 + 1}); 
      return;   
    } 
    
    log.info('validating ledgers:', end + 1, '-', validated);
    db.getLedgers({startIndex:end + 1, stopIndex:validated}, function(err, ledgers) {
      if (err) {
        callback(err);
        return;
      }
      
      if (!ledgers.length) {
        self._findGap(end, ledgerHash, startIndex || validated, callback); 
        return;
      }
      
      var index = validated - 1;
      for (var i=0; i<ledgers.length; i++) {  
        if (startIndex) {
          log.info("gap ends at:", ledgers[i].ledger_index);
          callback(null, {startIndex:startIndex, stopIndex:ledgers[i].ledger_index + 1});
          return;

        } else if (ledgers[i].ledger_index < index) {
          log.info("missing ledger at:", index); 
          log.info("gap ends at:", ledgers[i].ledger_index);
          callback(null, {startIndex:index, stopIndex:ledgers[i].ledger_index + 1});
          return;

        } else if (ledgerHash && ledgerHash !== ledgers[i].ledger_hash) {
          log.info("incorrect ledger hash at:", ledgers[i].ledger_index); 
          callback(null, {startIndex:index, stopIndex:ledgers[i].ledger_index + 1});
          return;

        } else {
          ledgerHash = ledgers[i].parent_hash;
          index = ledgers[i].ledger_index - 1;
        }
      }    
      
      if (index > end) {
        startIndex = index;
        log.info("missing ledger at:", index); 
      }
      
      self._findGap(end, ledgerHash, startIndex, callback); 
    });
    
/*
    db.nano.fetch({keys:ids}, function(err, resp){
      if (err || !resp.rows) {
        callback(err);
        return;
      }
      
      for (var i=0; i<resp.rows.length; i++) {
        if (startIndex) { 
          if (resp.rows[i].doc) {
            stopIndex = parseInt(resp.rows[i].key, 10) + 1;
            log.info("gap ends at:", stopIndex);
            callback(null, {startIndex:startIndex, stopIndex:stopIndex});
            return;
          }
          
        } else {
          if (!resp.rows[i].doc) {
            startIndex = parseInt(resp.rows[i].key, 10);
            log.info("missing ledger at:", startIndex);
              
          } else if (ledgerHash && resp.rows[i].doc.ledger_hash !== ledgerHash) {
            startIndex = parseInt(resp.rows[i].key, 10);
            log.info("incorrect ledger hash at:", startIndex);  
            
          } else {
            ledgerHash = resp.rows[i].doc.parentHash;
          }
        } 
      }
      
      self._findGap(end, ledgerHash, startIndex, callback);   
    }); 
*/    
  };
}

db.migrate().then(function() {
  h = new HistoricalImport();
  h.start();
});