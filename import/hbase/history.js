var config   = require('../../config/import.config');
var Importer = require('../../storm/multilang/resources/src/lib/modules/ripple-importer');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var utils    = require('../../storm/multilang/resources/src/lib/utils.js');
var Promise  = require('bluebird');
var moment   = require('moment');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger
var EPOCH_OFFSET   = 946684800;

var HistoricalImport = function () {
  this.importer = new Importer({
    ripple : config.get('ripple')
  });

  this.count    = 0;
  this.total    = 0;
  this.section  = { };


  var log       = new Logger({
    scope : 'hbase_history',
    level : config.get('logLevel') || 0,
    file  : config.get('logFile')
  });

  var hbaseOptions = config.get('hbase');
  var self = this;
  var stopIndex;
  var cb;

  hbaseOptions.logLevel = 2;
  this.hbase = new Hbase(hbaseOptions);

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

            self._findGaps(stopIndex, self.section.stopIndex + 1);
          }
        }
      }
    });
  });


  this.start = function (stop, start, callback) {
    var self  = this;

    if (!stop || stop < GENESIS_LEDGER) {
      stop = GENESIS_LEDGER;
    }

    cb        = callback;
    stopIndex = stop;

    log.info("starting historical import: ", stop, start);

    if (start && start !== 'validated') {
      self._findGaps(stop, start);

    //get latest validated ledger as the
    //stop point for historical importing
    } else {
      self._getLedgerRecursive('validated', 0, function(err, ledger) {
        if (err) {
          log.error('failed to get latest validated ledger');
          callback('failed to get latest validated ledger');
          return;
        }

        startIndex = parseInt(ledger.ledger_index, 10) - 1;
        self._findGaps(stop, startIndex);
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


  this._findGaps = function (stop, start) {
    log.info("finding gaps from ledgers:", stop, start);
    var self = this;

    this._findGap({
      index      : start,
      start      : start,
      stop       : stop
    }, function(err, resp) {
      if (err) {
        log.error(err);

      } else if (resp) {
        self.importer.backFill(resp.stopIndex, resp.startIndex);
        self.count   = 0;
        self.total   = resp.startIndex - resp.stopIndex + 1;
        self.section = resp;
      }
    });
  };

  this._findGap = function (params, callback) {
    var self = this;
    var end        = params.index - 200;
    var startIndex = params.index;
    var stopIndex  = end;
    var ledgerHash = params.ledger_hash;

    if (params.stop && end < params.stop) {
      end = params.stop;
    }

    log.info('validating ledgers:', end, '-', startIndex);

    self.hbase.getLedgersByIndex({
      startIndex : end,
      stopIndex  : startIndex,
      descending : true
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
        if (ledgers[i].ledger_index === startIndex + 1) {
          log.info('duplicate ledger index:', ledgers[i].ledger_index);
          return;

        } else if (ledgers[i].ledger_index !== startIndex) {
          log.info('missing ledger at:', startIndex);
          log.info("gap ends at:", ledgers[i].ledger_index);
          callback(null, {startIndex:startIndex, stopIndex:ledgers[i].ledger_index});
          return;

        } else if (ledgerHash && ledgerHash !== ledgers[i].ledger_hash) {
          log.info('incorrect ledger hash at:', startIndex);
          callback(null, {startIndex:startIndex, stopIndex:startIndex});
          return;
        }

        ledgerHash = ledgers[i].parent_hash;
        startIndex--;
      }


      if (end > params.stop) {
        self._findGap({
          index : startIndex,
          stop  : params.stop
        }, callback);

      } else {
        log.info("stop index reached: ", params.stop);
        callback(null, null);
        if (cb) cb();
        return;
      }
    });
  };

  function saveLedger (ledger, callback) {
    var parsed = Parser.parseLedger(ledger);

    self.hbase.saveParsedData({data:parsed}, function(err, resp) {
      if (err) {
        callback('unable to save parsed data for ledger: ' + ledger.ledger_index);
        return;
      }

      log.info('parsed data saved: ', ledger.ledger_index);

      self.hbase.saveTransactions(parsed.transactions, function(err, resp) {
        if (err) {
          callback('unable to save transactions for ledger: ' + ledger.ledger_index);
          return;
        }

        log.info(parsed.transactions.length + ' transactions(s) saved: ', ledger.ledger_index);

        self.hbase.saveLedger(parsed.ledger, function(err, resp) {
          if (err) {
            log.error(err);
            callback('unable to save ledger: ' + ledger.ledger_index);

          } else {

            log.info('ledger saved: ', ledger.ledger_index);
            callback(null, true);
          }
        });
      });
    });
  }
};

module.exports = HistoricalImport;
