var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account tx'});
var moment = require('moment');
var response = require('response');
var intMatch = /^\d+$/;
var hbase;

var txTypes = [
  'Payment',
  'OfferCreate',
  'OfferCancel',
  'AccountSet',
  'SetRegularKey',
  'TrustSet',
  'EnableAmendment',
  'SetFee'
];

var txResults = [
  'tesSUCCESS',
  'tecCLAIM',
  'tecPATH_PARTIAL',
  'tecUNFUNDED_ADD',
  'tecUNFUNDED_OFFER',
  'tecUNFUNDED_PAYMENT',
  'tecFAILED_PROCESSING',
  'tecDIR_FULL',
  'tecINSUF_RESERVE_LINE',
  'tecINSUF_RESERVE_OFFER',
  'tecNO_DST',
  'tecNO_DST_INSUF_XRP',
  'tecNO_LINE_INSUF_RESERVE',
  'tecNO_LINE_REDUNDANT',
  'tecPATH_DRY',
  'tecUNFUNDED',
  'tecMASTER_DISABLED',
  'tecNO_REGULAR_KEY',
  'tecOWNERS',
  'tecNO_ISSUER',
  'tecNO_AUTH',
  'tecNO_LINE',
  'tecINSUFF_FEE',
  'tecFROZEN',
  'tecNO_TARGET',
  'tecNO_PERMISSION',
  'tecNO_ENTRY',
  'tecINSUFFICIENT_RESERVE'
];

var accountTransactions = function (req, res) {
  var options = {
    account: req.params.address,
    type: req.query.type,
    result: req.query.result,
    binary: (/true/i).test(req.query.binary) ? true : false,
    minSequence: req.query.min_sequence,
    maxSequence: req.query.max_sequence,
    limit: req.query.limit || 20,
    descending: (/true/i).test(req.query.descending) ? true : false
  };

  if (options.minLedger && !intMatch.test(options.minLedger)) {
    errorResponse({error: 'invalid ledger_min', code: 400});
    return;
  }

  if (options.maxLedger && !intMatch.test(options.maxLedger)) {
    errorResponse({error: 'invalid ledger_max', code: 400});
    return;
  }

  if (isNaN(options.limit)) {
    options.limit = 20;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

  // query by sequence #
  if (options.minSequence || options.maxSequence) {
    if (options.minSequence && !intMatch.test(options.minSequence)) {
      errorResponse({error: 'invalid min_sequence', code: 400});
      return;
    }

    if (options.maxSequence && !intMatch.test(options.maxSequence)) {
      errorResponse({error: 'invalid max_sequence', code: 400});
      return;
    }

  // query by date
  } else {
    options.start = moment.utc(req.query.start || '2013-01-01', moment.ISO_8601);
    options.end = moment.utc(req.query.end || (new Date).toISOString(), moment.ISO_8601);

    if (!options.start.isValid()) {
      errorResponse({
        error: 'invalid start date, format must be ISO 8601',
        code: 400
      });
      return;

    } else if (!options.end.isValid()) {
      errorResponse({
        error: 'invalid end date, format must be ISO 8601',
        code: 400
      });
      return;
    }
  }

  // require valid transaction type
  if (options.type && txTypes.indexOf(options.type) === -1) {
    errorResponse({
      error: 'invalid transaction type',
      code: 400
    });
    return;
  }

  // require valid tx_result
  if (options.result && txResults.indexOf(options.result) === -1) {
    errorResponse({
      error: 'invalid transaction result',
      code: 400
    });
    return;
  }

  log.info('ACCOUNT TX:', options.account);

  hbase.getAccountTransactions(options, function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp);
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
        message: 'unable to retrieve transactions'
      }).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} data
  */
  function successResponse (resp) {
    var result = {result : 'success'};

    result.count = resp.rows.length;
    result.marker = resp.marker;
    result.transactions = resp.rows;
    log.info('Transactions Found:', resp.rows.length);

    response.json(result).pipe(res);
  }
};

module.exports = function(db) {
  hbase = db;
  return accountTransactions;
};
