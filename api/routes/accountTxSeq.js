var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'account tx sequence'});
var response = require('response');
var postgres;

var accountTxSeq = function (req, res, next) {

  var options = prepareOptions();

  log.info('ACCOUNT TX:', options.account);

  postgres.getAccountTxSeq(options, function(err, transactions) {
    if (err) {
      errorResponse(err);
    } else if (transactions.length === 0) {
      errorResponse({error: "transaction not found", code:404})
    } else {
      successResponse(transactions[0]);
    }
  });

 /**
  * prepareOptions
  * parse request parameters to determine query options
  */
  function prepareOptions () {
    var options = {
      account  : req.params.address,
      sequence : req.params.sequence,
      binary   : !req.query.binary || (/false/i).test(req.query.binary) ? false : true
    };

    return options;
  };

 /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    if (err.code.toString()[0] === '4') {
      log.error(err.error || err);
      response.json({result:'error', message:err.error}).status(err.code).pipe(res);
    } else {
      response.json({result:'error', message:'unable to retrieve transaction'}).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */
  function successResponse (tx) {
    var result = {
      result      : 'success',
      transaction : tx
    };

    log.info('ACCOUNT TX: Transaction Found');
    response.json(result).pipe(res);
  };
}

module.exports = function(db) {
  postgres = db;
  return accountTxSeq;
};
