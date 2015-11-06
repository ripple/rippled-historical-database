var config   = require('../../config/import.config');
var Importer = require('../../lib/ripple-importer');
var Logger   = require('../../lib/logger');
var Hbase    = require('../../lib/hbase/hbase-client');
var Parser   = require('../../lib/ledgerParser');
var utils    = require('../../lib/utils.js');
var Promise  = require('bluebird');
var moment   = require('moment');

var GENESIS_LEDGER = 32570; // https://ripple.com/wiki/Genesis_ledger
var EPOCH_OFFSET   = 946684800;

var HistoricalImport = function () {
  this.importer = new Importer({
    ripple : config.get('ripple'),
    logLevel : config.get('logLevel') || 0,
    logFile  : config.get('logFile')
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

          if (self.force) {
            if (cb) cb();

          } else if (self.section.error) {
            log.info("Error in section - retrying:", self.section.startIndex, '-', self.section.stopIndex);
            self._findGaps(self.section.startIndex, stopIndex);

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


  this.start = function (start, stop, force, callback) {
    var self = this;

    if (!start || start < GENESIS_LEDGER) {
      start = GENESIS_LEDGER;
    }

    cb = callback;
    stopIndex = stop;
    self.force = force;

    log.info("starting historical import: ", start, stop);

    if (stop && stop !== 'validated') {

      if (force) {
        self.total = stop - start;
        self.importer.backFill(start, stop, function(err) {
          if (err) log.error(err);
        });
      } else {
        self._findGaps(start, stop);
      }
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
        if (force) {
          self.total = stopIndex - start;
          self.importer.backFill(start, stopIndex, function(err) {
            if (err) log.error(err);
          });

        } else {
          self._findGaps(start, stopIndex);
        }
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
        self.importer.backFill(resp.startIndex, resp.stopIndex, function(err) {
          if (err) {
            if (cb) cb(err);
          }
        });

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

    self.hbase.getLedgersByIndex({
      startIndex : startIndex,
      stopIndex  : end,
      descending : false
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
        if (ledgers[i].ledger_index === startIndex - 1) {
          log.info('duplicate ledger index:', ledgers[i].ledger_index);
          if (cb) cb();
          return;

        } else if (ledgers[i].ledger_index !== startIndex) {
          log.info('missing ledger at:', startIndex);
          log.info("gap ends at:", ledgers[i].ledger_index);
          callback(null, {startIndex:startIndex, stopIndex:ledgers[i].ledger_index});
          return;

        } else if (ledgerHash && ledgerHash !== ledgers[i].parent_hash) {
          log.info('incorrect parent hash at:', startIndex);
          callback(null, {startIndex:startIndex-1, stopIndex:startIndex});
          return;
        }


        ledgerHash = ledgers[i].ledger_hash;
        startIndex++;
      }


      if (end < params.stop) {
        self._findGap({
          index       : startIndex,
          stop        : params.stop,
          ledger_hash : ledgerHash
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
