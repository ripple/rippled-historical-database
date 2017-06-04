var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account tx by sequence'});
var intMatch = /^\d+$/;
var hbase = require('../../lib/hbase')

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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve transaction'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */
  function successResponse (tx) {
    log.info('Transaction Found:', tx.hash);
    res.json({
      result: 'success',
      transaction : tx
    });
  };
}

module.exports = accountTxSeq
