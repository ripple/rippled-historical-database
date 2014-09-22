var config   = require('../config/import.config');
var log      = require('../lib/log')('couchdb');
var moment   = require('moment');
var diff     = require('deep-diff');
var ripple   = require('ripple-lib');
var Import   = require('./importer');
var live     = new Import(config);
var history  = new Import(config);
var store    = require('node-persist');
var indexer  = require('./indexer');
var db       = require('../lib/couchdb')(config.get('nosql:db'));
var http     = require('http');
var https    = require('https');
var maxSockets;
var queries = 0;
var reset = process.argv.indexOf('--reset') !== -1 ? true : false;
var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

//this is the maximum number of concurrent requests to couchDB
maxSockets = config.get('maxSockets') || 200;
http.globalAgent.maxSockets = https.globalAgent.maxSockets = maxSockets;
store.initSync();

var saveLedger = function (ledger, callback) {
  var self = this;
  
  ledger = formatRemoteLedger(ledger);
  
  db.head(ledger._id, function(err, resp, headers) {
    if (err && err.status_code === 404) {
      saveLedgerRecursive(ledger, 0 , callback);
    
    } else if (headers && headers.etag) {
      ledger._rev = headers.etag.replace(/\"/g, "");
      log.info("Replacing ledger:", ledger.ledger_index);
      saveLedgerRecursive(ledger, 0 , callback);
      
    } else { 
      callback(err && err.description ? err.description : err || "error");
    } 
  });
}

var saveLedgerRecursive = function (ledger, attempts, callback) {
  var self = this;
  
  if (!attempts) attempts = 0;
  else if (attempts>10) {
    log.error("unable to save ledger batch");
    return;
    
  } else {
    log.info('retrying - attempts:', attempts);
  }
  
  log.info('['+new Date().toISOString()+']', 'saving ledger:', ledger.ledger_index);    
  log.info('queries:', ++queries);
  
  db.insert(ledger, function (err, resp) {
    queries--;
    if (err && err.status_code === 409) {
      log.info('document already saved:', ledger.ledger_index);
      if (typeof callback === 'function') callback(null, ledger);
      return;
      
    } else if (err) {
      log.info('error saving ledger:', ledger.ledger_index,  err.description ? err.description : err);
      saveLedgerRecursive(ledger, ++attempts, callback);
      return;
    } 
    
    log.info('['+new Date().toISOString()+']', 'ledger:', ledger.ledger_index, "saved");
    if (typeof callback === 'function') callback(null, ledger);
  });   
}



/**
* addLeadingZeros converts numbers to strings and pads them with
* leading zeros up to the given number of digits
*/
var addLeadingZeros = function (number, digits) {
  var numStr = String(number);
  if (!digits) digits = 10;
  while(numStr.length < digits) {
    numStr = '0' + numStr;
  }

  return numStr;
}

/**
*  formatRemoteLedger makes slight modifications to the
*  ledger json format, according to the format used in the CouchDB database
*/
var formatRemoteLedger = function(ledger) {

  ledger.close_time_rpepoch   = ledger.close_time;
  ledger.close_time_timestamp = ripple.utils.toTimestamp(ledger.close_time);
  ledger.close_time_human     = moment(ripple.utils.toTimestamp(ledger.close_time))
    .utc().format('YYYY-MM-DD HH:mm:ss Z');
  ledger.from_rippled_api = true;

  delete ledger.close_time;
  delete ledger.hash;
  delete ledger.accepted;
  delete ledger.totalCoins;
  delete ledger.closed;
  delete ledger.seqNum;

  // parse ints from strings
  ledger.ledger_index = parseInt(ledger.ledger_index, 10);
  ledger.total_coins = parseInt(ledger.total_coins, 10);

  // add exchange rate field to metadata entries
  ledger.transactions.forEach(function(transaction) {
    if(!transaction.metaData || !transaction.metaData.AffectedNodes) {
      log.error('transaction in ledger: ' + ledger.ledger_index + ' does not have metaData');
      return;
    }

    transaction.metaData.AffectedNodes.forEach(function(affNode) {

      var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;

      if (node.LedgerEntryType !== 'Offer') {
        return;
      }

      var fields = node.FinalFields || node.NewFields;

      if (typeof fields.BookDirectory === 'string') {
        node.exchange_rate = ripple.Amount.from_quality(fields.BookDirectory).to_json().value;
      }

    });
  });

  ledger._id = addLeadingZeros(ledger.ledger_index);
  return ledger;
}

var countSockets = function () {
  var count = 0;
  for (var key1 in http.globalAgent.sockets) {
    count += http.globalAgent.sockets[key1].length;
  }
    
  for (var key2 in https.globalAgent.sockets) {
    count += https.globalAgent.sockets[key2].length;
  } 
  
  return count; 
} 
 

var HistoricalImport = function () {
  this.importer = new Import(config);
  this.count    = 0;
  this.total    = 0;
  this.section  = { };
  this.first    = reset ? null : store.getItem('earliestValidated');
  var self      = this;
  
  
 /**
  * handle ledgers from the importer
  */  
  this.importer.on('ledger', function(ledger) {
    saveLedger(ledger, function(err, resp) {
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
            self._findGaps(self.section.startIndex, null);
            
          } else {
            store.setItem('earliestValidated', {index:self.section.stopIndex, hash:self.section.stopHash});
            log.info("gap filled:", self.section.stopIndex, '-', self.section.startIndex);
            self._findGaps(self.section.stopIndex, self.section.stopHash);
          }
        }      
      }
    });
  });
  
  this.start = function () {
    console.log(self.first);
    if (self.first) {
      self._findGaps(self.first.index, self.first.hash); 
       
    } else {
      
      //get latest validated ledger as the 
      //starting point for historical importing
      self._getLedgerRecursive('validated', 0, function(err, ledger) {
        if (err) {
          log.error("failed to get latest validated ledger");
          return;
        } 
        
        self._findGaps(ledger.ledger_index, ledger.ledger_hash);
      });
    }    
  };
};


HistoricalImport.prototype._getLedgerRecursive = function(index, attempts, callback) {
    if (attempts && attempts > 10) {
      callback("failed to get ledger");
      return;
    }
    
    this.importer.getLedger({index:index}, function(err, ledger) {
      if (err) {
        log.error(err, "retrying");
        this._getLedgerRecursive(index, ++attempts, callback);
        return;
      } 
      
      callback(null, ledger);
    });  
  },
  

HistoricalImport.prototype._findGaps = function (start, parentHash) {
  log.info("finding gaps from ledger:", start); 
  var self = this;
  
  this._findGap(start, parentHash, null, function(err, resp) {
    if (resp) {
      self.importer.backFill(resp.stopIndex, resp.startIndex);
      self.count   = 0;
      self.total   = resp.startIndex - resp.stopIndex + 1;
      self.section = resp;
    }
  });
}

HistoricalImport.prototype._findGap = function (validated, ledgerHash, startIndex, callback) {
  var self = this;
  var end  = validated - 20;
  var ids  = [];
  var stopIndex; 

  if (startIndex && startIndex - validated > 100) {
    log.info("max gap size reached:", startIndex);
    callback(null, {startIndex:startIndex, stopIndex:startIndex - 100 + 1}); 
    return;   
  } 
  
  for (var i = validated; i >= end; i--) {
    ids.push(addLeadingZeros(i));
  }
  
  log.info('validating ledgers:', end + 1, '-', validated);
  db.fetch({keys:ids}, function(err, resp){
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
}

live.liveStream();
live.on('ledger', function(ledger) {
  saveLedger(ledger, function(err, resp){
    if (resp) indexer.pingCouchDB();
  });
});

h = new HistoricalImport();
h.start();
  