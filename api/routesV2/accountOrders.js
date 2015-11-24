'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account orders'});
var request = require('request');
var smoment = require('../../lib/smoment');
var config = require('../../config/api.config');
var ripple = require('ripple-lib');
var rippleAPI = new ripple.RippleAPI(config.get('ripple'));
var hbase;

rippleAPI.connect();

function accountOrders(req, res) {
  var options = {
    ledger_index: req.query.ledger_index || req.query.ledger,
    ledger_hash: req.query.ledger_hash,
    closeTime: req.query.close_time || req.query.date,
    account: req.params.address,
    format: (req.query.format || 'json').toLowerCase(),
    limit: req.query.limit || 200
  };

  if (!options.account) {
    errorResponse({
      error: 'account is required.',
      code: 400
    });
    return;
  }

  // validate and fomat close time
  if (options.closeTime) {
    options.closeTime = smoment(options.closeTime);
    if (options.closeTime) {
      options.closeTime = options.closeTime.format();
    } else {
      errorResponse({
        error: 'invalid date format',
        code: 400
      });
      return;
    }
  }

  // validate and format limit
  if (options.limit && options.limit === 'all') {
    options.limit = undefined;

  } else {
    options.limit = Number(options.limit);
    if (isNaN(options.limit)) {
      errorResponse({
        error: 'invalid limit',
        code: 400
      });
      return;

    // max limit of 400
    } else if (options.limit > 400) {
      options.limit = 400
    }
  }


  log.info(options.account);

  hbase.getLedger(options, function(err, ledger) {
    if (err) {
      errorResponse(err);
      return;

    } else if (ledger) {
      options.ledger_index = ledger.ledger_index;
      options.closeTime = smoment(ledger.close_time).format();
      options.currency = req.query.currency;
      options.counterparty = req.query.counterparty || req.query.issuer;
      options.limit = options.limit;
      getOrders(options);

    } else {
      errorResponse('ledger not found');
    }
  });

  /**
  * getOrders
  * use ledger_index from getLedger api call
  * to get orders using rippleAPI
  */

  function getOrders(opts) {
    var params = {
      ledgerVersion: opts.ledger_index,
      limit: opts.limit
    };

    rippleAPI.getOrders(opts.account, params)
    .then(function(orders) {
      var results = {
        result: 'success'
      };

      results.ledger_index = opts.ledger_index;
      results.close_time = opts.closeTime;
      results.limit = opts.limit;
      results.orders = orders;

      successResponse(results, opts);
    }).catch(function(e) {
      if (e.message === 'Account not found.') {
        errorResponse({code:404, error: e.message});
      } else {
        errorResponse(e.toString());
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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve orders'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} balances
  */

  function successResponse(results, opts) {
    if (opts.format === 'csv') {
      res.csv(results.orders, opts.account + ' - orders.csv');
    } else {
      res.json(results);
    }
  }
}

module.exports = function(db) {
  hbase = db;
  return accountOrders;
};
