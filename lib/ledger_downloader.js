var request = require('request');

var SERVER = 'http://s1.ripple.com:51234';

function LedgerDownloader(options) {
  if (!options) { options = {} };
  this.server = options.server || SERVER;
};

LedgerDownloader.prototype = {
  _requestLedgerFromRippled: function(ledgerIndex, callback) {
    var self = this;

    var rpcRequestData = { 
      'method': 'ledger'
    };

    if (ledgerIndex) { 
      // No index specified
      rpcRequestData.params = [{ 
        'transactions' : true, 
        'expand'       : true,
        'ledger_index' : ledgerIndex
      }];
    }
    
    request({
      url: self.server,
      method: 'POST',
      json: rpcRequestData,
      timeout : 10000,
    }, function(error, response) {
      if (error) {
        callback(error, null);
      } else {
        callback(null, response.body);
      }
    });
  },

  latestIndex: function(callback) {
    this._requestLedgerFromRippled(null, function(error, body) {
      if (error) {
        callback(error, null);
      } else {
        // TODO: Catch any error messages
        callback(null, parseInt(body.result.closed.ledger.ledger_index));
      }
    });
  },

  getIndex: function(ledgerIndex, callback) {
    if (!ledgerIndex) callback(new Error('ledgerIndex required.'), null);
    this._requestLedgerFromRippled(ledgerIndex, function(error, body) {
      if (error) {
        callback(error, null);
      } else {
        // TODO: Catch any error messages
        callback(null, body.result.ledger);
      }
    });
  }
};

module.exports = LedgerDownloader;

