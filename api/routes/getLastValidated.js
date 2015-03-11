var response = require('response');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'last validated'});
var postgres;
var Validated;

Validated = function(req, res, next) {

  log.info('get last validated ledger');

  if (!postgres) {
    response.json({result:'error', message:'unavailable'}).status(500).pipe(res);
    return;
  }

  postgres.getLastValidated(function(err, resp) {
    if (err) {
      response.json({result:'error', message:'unavailable'}).status(500).pipe(res);

    } else if (resp) {
      var result = {
        result       : 'success',
        ledger_index : resp.ledger_index,
        ledger_hash  : resp.ledger_hash.toUpperCase(),
        parent_hash  : resp.parent_hash.toUpperCase()
      };

      response.json(result).pipe(res);
    }
  });
};

module.exports = function(db) {
  postgres = db;
  return Validated;
};
