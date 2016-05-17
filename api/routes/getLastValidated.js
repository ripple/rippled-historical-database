var Logger = require('../../lib/logger');
var log = new Logger({scope : 'last validated'});
var smoment = require('../../lib/smoment');
var hbase;

var getLastValidated = function(req, res, next) {

  log.info('get last validated ledger');

  hbase.getLastValidated(function(err, resp) {
    if (err) {
      res.status(500).json({
        result: 'error',
        message: 'unavailable'
      });

    } else if (resp) {
      res.json({
        result: 'success',
        ledger_index: resp.ledger_index,
        ledger_hash: resp.ledger_hash,
        parent_hash: resp.parent_hash,
        close_time: smoment(resp.close_time).format()
      });
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return getLastValidated;
};
