'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'top currencies'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase;

var getTopCurrencies = function(req, res) {
  var options = {
    date: req.params.date ? smoment(req.params.date) : undefined,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.params.date && !options.date) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    });
    return;

  } else if (options.date) {
    options.date.moment.startOf('day');
  }

  hbase.getTopCurrencies(options, function(err, markets) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(markets, options);
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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve top currencies'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(markets, options) {
    var date = options.date ?
        options.date.format() : smoment().format();

    if (options.format === 'csv') {
      filename = 'top currencies - ' + date + '.csv';
      res.csv(markets, filename);

    } else {
      res.json({
        result: 'success',
        date: date,
        count: markets.length,
        markets: markets
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return getTopCurrencies;
};
