'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'accounts'});
var response = require('response');
var hbase;

/**
 * getAccount
 */

var getAccount = function(req, res, next) {
  var options;

 /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse(err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      response.json({
        result: 'error',
        message: err.error
      }).status(err.code).pipe(res);
    } else {
      response.json({
        result: 'error',
        message: 'unable to get accounts'
      }).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {
    var result = {
      result: 'success',
      account: {
        address: resp.rows[0].account,
        parent: resp.rows[0].parent,
        initial_balance: resp.rows[0].balance,
        inception: resp.rows[0].executed_time,
        ledger_index: resp.rows[0].ledger_index,
        tx_hash: resp.rows[0].tx_hash,
        genesis_balance: resp.rows[0].genesis_balance,
        genesis_index: resp.rows[0].genesis_index
      }
    };

    response.json(result).pipe(res);
  }

  options = {
    account: req.params.address,
    limit: 1,
    descending: false // query will be faster
  };

  hbase.getAccounts(options, function(err, resp) {
    if (err) {
      errorResponse(err);
    } else if (!resp || !resp.rows || !resp.rows.length) {
      errorResponse({error: 'Account not found', code: 404});
    } else {
      successResponse(resp);
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return getAccount;
};
