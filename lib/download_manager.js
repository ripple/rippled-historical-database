var LedgerDownloader = require('./ledger_downloader');
var Ledger = require('./models/ledger');
var _ = require('underscore');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

function DownloadManager() {
  this.ld = new LedgerDownloader();
};

DownloadManager.prototype = {

  _getLedger: function(index, callback) {
    var _this = this;

    this.ld.getLedger(index, function(error, ledger) {
      //console.log(index, ledger);

      if (!error) {
        // Save ledger to database
        Ledger.createFromJSON(ledger, function(error, ledger){
          if (ledger) console.log('LEDGER: ', ledger.id);
          callback(error);
        });
      }
      else {
        callback(error);
      }
    });
  },

  /**
   * Returns array of unsaved index id's
   */
  _getIndexes: function(callback) {

    // Get the latest index
    this.ld.latestIndexNumber(function(error, latest) {
      if (!error) {
        var all_indexes = [];
        for (var i = GENESIS_LEDGER; i < latest; i++) {
          all_indexes.push(i);
        }

        Ledger.setOfSavedLedgers(function(error, indexes) {
          if (!error) {
            var saved_indexes = _.map(indexes, function(index) {
              return parseInt(index.id);
            });

            var needed_indexes = _.difference(all_indexes, saved_indexes);
            callback(null, needed_indexes);
          }
          else {
            callback(error);
          }
        });
      }
      else {
        callback(error);
      }
    });
  },

  _recursiveLedger: function(indexes) {
    var _this = this;

    var length = indexes.length;
    if(length <= 0) {
      // Completed download
      console.log('Completed Download');
      return;
    }

    // Get and save ledger
    var index = indexes[length - 1];
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
      indexes.pop();
      _this._recursiveLedger(indexes);
    });
  },

  start: function() {
    var _this = this;

    this._getIndexes(function(error, indexes) {
      if (error) throw error;

      _this._recursiveLedger(indexes);
    });
  }
};

module.exports = DownloadManager;
