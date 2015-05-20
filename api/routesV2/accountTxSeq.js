var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account tx by sequence'});
var response = require('response');
var intMatch = /^\d+$/;
var hbase;

var accountTxSeq = function (req, res, next) {

  var options = {
    account: req.params.address,
    sequence: req.params.sequence,
    binary: (/true/i).test(req.query.binary) ? true : false
  };

  if (!intMatch.test(options.sequence)) {
    errorResponse({error: 'invalid sequence number', code: 400});
    return;
  }

  log.info('ACCOUNT TX SEQ:', options.account, options.sequence);

  hbase.getAccountTransaction(options, function(err, tx) {
    if (err) {
      errorResponse(err);
    } else if (!tx) {
      errorResponse({error: 'transaction not found', code: 404})
    } else {
      successResponse(tx);
    }
  });

 /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      response.json({
        result: 'error',
        message: err.error
      }).status(err.code).pipe(res);
    } else {
      response.json({
        result: 'error',
        message: 'unable to retrieve transaction'
      }).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */
  function successResponse (tx) {
    log.info('Transaction Found:', tx.hash);
    response.json({
      result: 'success',
      transaction : tx
    }).pipe(res);
  };
}

module.exports = function(db) {
  hbase = db;
  return accountTxSeq;
};
