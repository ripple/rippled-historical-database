'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'get payments'});
var smoment = require('../../lib/smoment');
var response = require('response');
var hbase;

var getExchanges = function(req, res) {

  var options = {
    date: smoment(req.query.date),
    base: {},
    counter: {}
  };

  var base = req.params.base.split(/[\+|\.]/); //any of +, |, or .
  var counter = req.params.counter.split(/[\+|\.]/);

  options.base.currency = base[0] ? base[0].toUpperCase() : undefined;
  options.base.issuer = base[1] ? base[1] : undefined;

  options.counter.currency = counter[0] ? counter[0].toUpperCase() : undefined;
  options.counter.issuer = counter[1] ? counter[1] : undefined;

  if (!options.base.currency) {
    errorResponse({error:'base currency is required', code:400});
    return;
  } else if (!options.counter.currency) {
    errorResponse({error: 'counter currency is required', code: 400});
    return;
  } else if (options.base.currency === 'XRP' && options.base.issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.counter.currency === 'XRP' && options.counter.issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.base.currency !== 'XRP' && !options.base.issuer) {
    errorResponse({error: 'base issuer is required', code: 400});
    return;
  } else if (options.counter.currency !== 'XRP' && !options.counter.issuer) {
    errorResponse({error: 'counter issuer is required', code: 400});
    return;
  }

  hbase.getExchangeRate(options)
  .nodeify(function(err, rate) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(rate);
    }
  });

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

  function successResponse(rate) {
    response.json({
      result: 'success',
      rate: rate
    }).pipe(res);
  }
};


module.exports = function(db) {
  hbase = db;
  return getExchanges;
};
