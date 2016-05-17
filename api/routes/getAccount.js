'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'accounts'});
var smoment = require('../../lib/smoment');
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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to get accounts'
      });
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
      account_data: {
        account: resp.rows[0].account,
        parent: resp.rows[0].parent,
        initial_balance: resp.rows[0].balance,
        inception: smoment(resp.rows[0].executed_time).format(),
        ledger_index: resp.rows[0].ledger_index,
        tx_hash: resp.rows[0].tx_hash,
        genesis_balance: resp.rows[0].genesis_balance,
        genesis_index: resp.rows[0].genesis_index
      }
    };

    res.json(result);
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
