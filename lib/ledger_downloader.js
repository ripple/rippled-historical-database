var request = require('request');

var SERVER = 'http://s1.ripple.com:51234';

function LedgerDownloader(options) {
  if (!options) { options = {} };
  this.server = options.server || SERVER;
};

LedgerDownloader.prototype = {
  _requestFromRippled: function(rpcRequestData, callback) {
    request({
      url: this.server,
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

  latestIndexNumber: function(callback) {
    var rpcRequestData = {
      method: 'ledger_current'
    };

    this._requestFromRippled(rpcRequestData, function(error, body) {
      if (error) {
        callback(error, null);
      } else {
        // TODO: Catch any error messages
        console.log(body);
        callback(null, parseInt(body.result.ledger_current_index));
      }
    });
  },

  getLedger: function(ledgerIndex, callback) {
    if (!ledgerIndex) callback(new Error('ledgerIndex required.'), null);

    var rpcRequestData = {
      method: 'ledger',
      params: [{
        transactions : true,
        expand       : true,
        ledger_index : ledgerIndex
      }]
    };

    this._requestFromRippled(rpcRequestData, function(error, body) {
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
