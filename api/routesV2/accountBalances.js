'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account balances'});
var request = require('request');
var response = require('response');
var smoment = require('../../lib/smoment');
var API = 'https://api.ripple.com/v1';
var hbase;

var accountBalances = function (req, res, next) {

  var options = {
    ledger_index: req.query.ledger_index || req.query.ledger,
    ledger_hash: req.query.ledger_hash,
    closeTime: smoment(req.query.close_time || req.query.date),
    account: req.params.address,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (!options.account) {
    errorResponse({
      error: 'account is required.',
      code: 400
    });
    return;
  }

  if (!options.closeTime) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    });
    return;

  } else {
    options.closeTime = options.closeTime.format();
  }

  log.info('ACCOUNT BALANCES:', options.account);

  hbase.getLedger(options, function(err, ledger) {
    if (err) {
      errorResponse(err);
      return;
    } else if (ledger) {
      options.ledger_index = ledger.ledger_index;
      options.closeTime = smoment(ledger.close_time).format();
    }

    options.currency = req.query.currency;
    options.counterparty = req.query.counterparty || req.query.issuer;
    options.limit = req.query.limit;
    options.marker = req.query.marker;
    getBalances(options);
  });

  /**
  * getBalances
  * use ledger_index from getLedger api call
  * to get balances using ripple REST
  */

  function getBalances(opts) {
    var url = API + '/accounts/' + opts.account + '/balances';
    var results = {
      result: 'success',
    };

    request({
      url: url,
      json: true,
      qs: {
        currency: opts.currency,
        counterparty: opts.counterparty,
        limit: opts.limit,
        marker: opts.marker,
        ledger: opts.ledger_index
      }
    }, function(err, resp, body) {

      if (err) {
        errorResponse(err);
        return;
      }

      results.ledger_index = body.ledger;
      results.close_time = opts.closeTime;
      results.marker = body.marker;
      results.limit = body.limit;
      results.validated = body.validated;
      results.balances = body.balances;

      successResponse(results, opts);
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
      response.json({result: 'error', message: 'unable to retrieve ledger'})
        .status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} balances
  */

  function successResponse(balances, opts) {

    if (opts.format === 'csv') {
      res.csv(balances.balances, opts.account + ' - balances.csv');
    } else {
      response.json(balances).pipe(res);
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return accountBalances;
};
