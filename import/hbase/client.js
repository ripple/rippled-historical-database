var config   = require('../../config/import.config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var utils    = require('../../storm/multilang/resources/src/lib/utils.js');
var Promise  = require('bluebird');
var moment   = require('moment');

var Client = function () {
  var self = this;
  var log  = new Logger({
    scope : 'hbase_history',
    level : config.get('logLevel') || 0,
    file  : config.get('logFile')
  });

  var hbaseOptions = config.get('hbase');
  hbaseOptions.logLevel = 2;
  self.hbase = new Hbase(hbaseOptions);
  self.hbase.connect();

  self.saveLedger = function (ledger, callback) {

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

module.exports = Client;
