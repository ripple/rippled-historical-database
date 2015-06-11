'use strict';
var smoment = require('../../lib/smoment');
var Logger = require('../../lib/logger');
var log = new Logger({scope : 'get tx'});
var response = require('response');
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


var getTransactions = function (req, res, next) {
  var hexMatch = new RegExp('^(0x)?[0-9A-Fa-f]+$');
  var options  = {
    tx_hash: req.params.tx_hash,
    binary: (/true/i).test(req.query.binary) ? true : false,
    descending: (/true/i).test(req.query.descending) ? true : false,
    type: req.query.type,
    result: req.query.result,
    marker: req.query.marker,
    limit: req.query.limit || 20
  };

  // single TX
  if (options.tx_hash) {
    if (!hexMatch.test(options.tx_hash) ||
        options.tx_hash.length % 2 !== 0) {
      errorResponse({error: 'invalid hash', code:400});
      return;
    }

    log.info(options.tx_hash);
    hbase.getTransaction(options, function(err, tx) {
      if (err) {
        errorResponse(err);
      } else if (tx) {
        successResponse(tx);
      } else {
        errorResponse({
          error: 'transaction not found',
          code: 404
        });
      }
    });

  // transactions by time
  } else {
    options.start = smoment(req.query.start || '2013-01-01');
    options.end = smoment(req.query.end);

    if (!options.start) {
      errorResponse({
        error: 'invalid start time, must be ISO_8601',
        code: 400
      });
      return;

    } else if (!options.end) {
      errorResponse({
        error: 'invalid start time, must be ISO_8601',
        code: 400
      });
      return;
    }

    // max limit 100
    if (options.limit > 100) {
      options.limit = 100;
    }

    // require valid transaction type
    if (options.type && txTypes.indexOf(options.type) === -1) {
      errorResponse({
        error: 'invalid transaction type',
        code: 400
      });
      return;
    }

    // require valid transaction result
    if (options.result && txResults.indexOf(options.result) === -1) {
      errorResponse({
        error: 'invalid transaction result',
        code: 400
      });
      return;
    }

    options.include_ledger_hash = true;

    //log.info(options.start.format(), options.end.format());
    hbase.getTransactions(options, function(err, resp) {
      if (err) {
        errorResponse(err);
      } else {
        successResponse(resp);
      }
    });
  }


  /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse(err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      response.json({result: 'error', message: err.error})
        .status(err.code).pipe(res);
    } else {
      response.json({result: 'error', message: 'unable to retrieve transaction'})
        .status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */

  function successResponse(resp) {
    var result = {result: 'success'};
    if (resp.rows) {
      result.count = resp.rows.length;
      result.marker = resp.marker;
      result.transactions = resp.rows;
    } else {
      result.transaction = resp;
    }

    response.json(result).pipe(res);
  }

};

module.exports = function(db) {
  hbase = db;
  return getTransactions;
};
