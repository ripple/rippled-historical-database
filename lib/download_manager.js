var LedgerDownloader = require('./ledger_downloader');
var Ledger = require('./models/ledger');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

function DownloadManager() {
  this.ld = new LedgerDownloader();
};

DownloadManager.prototype = {
  _saveLedger: function(ledger, callback) {
    // Save ledger to database
    Ledger.create({
      id: ledger.ledger_index,
      hash: ledger.hash,
      sequence: ledger.seqHash,
      prev_hash: ledger.parent_hash,
      total_coins: ledger.total_coins,
      closing_time: ledger.close_time,
      //prev_closing_time: ledger.,
      close_time_resolution: ledger.close_time_resolution,
      //close_flags: ledger.,
      account_set_hash: ledger.account_hash
    }).complete(function(error, ledger){
      if (ledger) console.log('LEDGER: ', ledger.id);
      callback(error);
    });
  },

  _getLedger: function(index, callback) {
    var _this = this;

    this.ld.getLedger(index, function(error, ledger) {
      if (!error) {
        _this._saveLedger(ledger, callback);
      }
      else {
        callback(error);
      }
    });
  },

  _getIndex: function(callback) {

    // TODO: Check for missing ledgers

    var _this = this;
    Ledger.max('id').success(function(max) {
      max = parseInt(max);
      if (max > GENESIS_LEDGER) {
        callback(null, max + 1);
        // Get the latest index
        //_this.ld.latestIndexNumber(callback);
      }
      else {
        callback(null, GENESIS_LEDGER);
      }
    });
  },

  _recursiveLedger: function(index) {
    var _this = this;

    // Get and save ledger
    this._getLedger(index, function(error) {
      if (error) {
        throw error;

        // if (error.code === '23505') {
        //   // Duplicate ledger.
        //   // Get a new ledger index
        //   _this.start();
        //   return;
        // }
        // else {
        //   console.log(error);
        //   throw error;
        // }
      }
      _this._recursiveLedger(index + 1);
    });
  },

  start: function() {
    var _this = this;

    this._getIndex(function(error, index) {
      if (error) throw error;

      _this._recursiveLedger(index);
    });
  }
};

module.exports = DownloadManager;
