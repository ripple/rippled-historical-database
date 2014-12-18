var config   = require('../../config/import.config');
var log      = require('../../lib/log')('couchdb');
var moment   = require('moment');
var diff     = require('deep-diff');
var ripple   = require('ripple-lib');
var http     = require('http');
var https    = require('https');
var queries  = 0;
var dbConfig = config.get('nosql:db');
var nano     = require('nano')({
    url : dbConfig.protocol +
      '://' + dbConfig.username + 
      ':'   + dbConfig.password + 
      '@'   + dbConfig.host + 
      ':'   + dbConfig.port + 
      '/'   + dbConfig.database,
    request_defaults : {timeout :90 * 1000}, //90 seconds max for couchDB 
  });  


//this is the maximum number of concurrent requests to couchDB
var maxSockets = config.get('maxSockets') || 200;
http.globalAgent.maxSockets = https.globalAgent.maxSockets = maxSockets;

var Client = {
  nano : nano,
  
  saveLedger : function (ledger, callback) {
    var self = this;
    
    ledger = formatRemoteLedger(ledger);
    
    nano.head(ledger._id, function(err, resp, headers) {
      if (err && err.status_code === 404) {
        saveLedgerRecursive(ledger, 0 , callback);
      
      } else if (headers && headers.etag) {
        ledger._rev = headers.etag.replace(/\"/g, "");
        log.info("Replacing ledger:", ledger.ledger_index);
        saveLedgerRecursive(ledger, 0 , callback);
        
      } else { 
        var error = err && err.description ? err.description : err || "error";
        log.error("rev lookupp failed:", error);
        callback(error);
      } 
    });
  },
  
  getLatestLedger : function (callback) {
    var params = {
      endKey     : moment.utc().toArray().slice(0,6),
      limit      : 1,
      reduce     : false,
      descending : true
    };
    
    nano.view('ledgersClosed', 'v1', params, function(err, resp){
      if (err || !resp || !resp.rows || !resp.rows.length) {
        return callback(err);
      }
      
      nano.get(resp.rows[0].id, callback);
    });
  },
  
  /**
  * addLeadingZeros converts numbers to strings and pads them with
  * leading zeros up to the given number of digits
  */
  
  addLeadingZeros : function (number, digits) {
    var numStr = String(number);
    if (!digits) digits = 10;
    while(numStr.length < digits) {
      numStr = '0' + numStr;
    }
  
    return numStr;
  }  
};

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
  
  nano.insert(ledger, function (err, resp) {
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

  ledger._id = Client.addLeadingZeros(ledger.ledger_index);
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
 
module.exports = Client;
  