var LedgerDownloader = require('./ledger_downloader');
var DataParser = require('./data_parser');
var Ledger = require('./models/ledger');
var _ = require('underscore');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger

function DownloadManager() {
  this.ld = new LedgerDownloader();

  this.saved_ledgers = {};
  this.next_ledger = undefined;
  this.max_ledger = undefined;
};

DownloadManager.prototype = {

  _getLedger: function(index, callback) {
    var _this = this;

    //console.log('Getting ledger: ' + index + '...');

    this.ld.getLedger(index, function(error, ledger) {
      if (!error) {
        // Save ledger to database
        //console.log('SAVING DATA...');
        DataParser.parseAndSaveData(ledger, function(error, ledger){
          if (ledger) console.log('SAVED LEDGER: ', ledger.id);
          callback(error);
        });
      }
      else {
        callback(error);
      }
    });
  },

  /**
   * Returns object of saved ledger indexes
   */
  _getIndexes: function(callback) {

    console.log('Getting saved ledgers');
    Ledger.setOfSavedLedgers(function(error, indexes) {
      if (!error) {
        console.log('Pulling ids');
        var saved_ledgers = {};
        for (var i = 0; i < indexes.length; i++) {
          saved_ledgers[parseInt(indexes[i].id)] = true;
        }

        console.log('Done building saved ids');

        callback(null, saved_ledgers);
      }
      else {
        callback(error);
      }
    });
  },

  _recursiveLedger: function() {
    var _this = this;

    // Get and save ledger
    while (this.saved_ledgers[this.next_ledger]) {
      this.next_ledger = this.next_ledger - 1;

      if(this.next_ledger < GENESIS_LEDGER) {
        // Completed download
        console.log('Completed Download');
        return;
      }
    }

    this._getLedger(this.next_ledger, function(error) {
      if (error) {
        // TODO: Handle error
        //throw error;

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
      else {
        // Ledger saved
        _this.saved_ledgers[_this.next_ledger] = true;
      }

      setImmediate(function() {
        // Use setImmediate to unwind the call stack
        _this._recursiveLedger();
      });
    });
  },

  start: function() {
    var _this = this;

    this._getIndexes(function(error, indexes) {
      if (error) throw error;

      _this.saved_ledgers = indexes;

      // Get the latest index
      _this.ld.latestIndexNumber(function(error, latest) {
        if (error) throw error;

        _this.next_ledger = latest;
        _this.current_ledger = latest;

        console.log('Latest index: ' + latest);
        _this._recursiveLedger();
      });
    });
  }
};

module.exports = DownloadManager;
