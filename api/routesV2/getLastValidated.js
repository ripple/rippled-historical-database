var response = require('response');
var Logger = require('../../lib/logger');
var log = new Logger({scope : 'last validated'});
var hbase;

var getLastValidated = function(req, res, next) {

  log.info('get last validated ledger');

  hbase.getLastValidated(function(err, resp) {
    if (err) {
      response.json({
        result: 'error',
        message: 'unavailable'
      }).status(500).pipe(res);

    } else if (resp) {
      response.json({
        result: 'success',
        ledger_index: resp.ledger_index,
        ledger_hash: resp.ledger_hash,
        parent_hash: resp.parent_hash,
        close_time: resp.close_time
      }).pipe(res);
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return getLastValidated;
};
