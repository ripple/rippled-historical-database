'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'get payments'});
var smoment = require('../../lib/smoment');
var Promise = require('bluebird');
var response = require('response');
var hbase;

var normalize = function(req, res) {

  var options = {
    date: req.query.date,
    amount: Number(req.query.amount),
    currency: (req.query.currency || 'XRP').toUpperCase(),
    issuer: req.query.issuer || '',
    exchange_currency: (req.query.exchange_currency || 'XRP').toUpperCase(),
    exchange_issuer: req.query.exchange_issuer || ''
  };


  if (isNaN(options.amount)) {
    errorResponse({error: 'invalid amount', code: 400});
    return;
  } else if (!options.currency) {
    errorResponse({error: 'currency is required', code: 400});
    return;
  } else if (options.currency === 'XRP' && options.issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.currency !== 'XRP' && !options.issuer) {
    errorResponse({error: 'issuer is required', code: 400});
    return;
  } else if (options.exchange_currency === 'XRP' && options.exchange_issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.exchange_currency !== 'XRP' && !options.exchange_issuer) {
    errorResponse({error: 'issuer is required', code: 400});
    return;
  }

  if (options.currency === options.exchange_currency &&
      options.issuer === options.exchange_issuer) {
    successResponse({
      amount: options.amount,
      converted: options.amount,
      rate: 1
    });
    return;
  }

  if (!options.date) {
    options.date = smoment();
  }

  Promise.all([
    getXRPrate(),
    getExchangeRate()
  ])
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err);

    } else {
      var rate = resp[0] / resp[1];
      successResponse({
        amount: options.amount,
        converted: options.amount * rate,
        rate: rate
      });
    }
  });

  // conversion to XRP
  function getXRPrate() {
    if (options.currency === 'XRP') {
      return Promise.resolve(1);
    } else {
      return hbase.getExchangeRate({
        date: options.date,
        base: {
          currency: options.currency,
          issuer: options.issuer
        }
      });
    }
  }

  // conversion to exchange currency
  function getExchangeRate() {
    if (options.exchange_currency === 'XRP') {
      return Promise.resolve(1);
    } else {
      return hbase.getExchangeRate({
        date: options.date,
        base: {
          currency: options.exchange_currency,
          issuer: options.exchange_issuer
        }
      });
    }
  }


  //get daily to XRP
  //get last 50 to XRP in two weeks

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
      response.json({result: 'error', message: 'unable to retrieve exchanges'})
      .status(500).pipe(res);
    }
  }

  /**
   * successResponse
   * return a successful response
   * @param {Object} exchanges
   */

  function successResponse(data) {
    response.json({
      result: 'success',
      amount: data.amount,
      converted: data.converted,
      rate: data.rate
    }).pipe(res);
  }
};


module.exports = function(db) {
  hbase = db;
  return normalize;
};
