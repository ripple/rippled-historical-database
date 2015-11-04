'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'metrics'});
var smoment = require('../../../lib/smoment');
var response = require('response');
var utils = require('../../../lib/utils');
var hbase;
var table = 'agg_metrics';
var intervals = [
  'day',
  'week',
  'month'
];

function getMetric(metric, req, res) {
  var exchange = {
    currency : (req.query.exchange_currency || 'XRP').toUpperCase(),
    issuer: req.query.exchange_issuer
  };

  var options = {
    metric: metric,
    interval: req.query.interval,
    start: req.query.start ? smoment(req.query.start) : null,
    end: req.query.end ? smoment(req.query.end) : null,
    marker: req.query.marker,
    limit: Number(req.query.limit || 200),
    format: (req.query.format || 'json').toLowerCase()
  };

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

  if (exchange.currency !== 'XRP' && !exchange.issuer) {
    errorResponse({
      error: 'exchange currency must have an issuer',
      code: 400
    });
    return;

  } else if (exchange.currency === 'XRP' && exchange.issuer) {
    errorResponse({
      error: 'XRP cannot have an issuer',
      code: 400
    });
    return;

  } else if (exchange.currency !== 'XRP') {
    options.exchange = exchange;
  }

  // historical data
  if (req.query.start && !options.start) {
    errorResponse({
      error: 'invalid start date format',
      code: 400
    });
    return;

  } else if (req.query.end && !options.end) {
    errorResponse({
      error: 'invalid end date format',
      code: 400
    });
    return;

  } else if (options.interval &&
             intervals.indexOf(options.interval) === -1) {
    errorResponse({
      error: 'invalid interval - use: ' + intervals.join(', '),
      code: 400
    });
    return;

  } else if (options.interval && metric === 'issued_value') {
    errorResponse({
      error: 'interval cannot be used',
      code: 400
    });
    return;

  } else if (options.start || options.end) {
    if (!options.start) {
      options.start = smoment(0);
    }
    if (!options.end) {
      options.end = smoment();
    }


  // rolling 24 hr
  } else {
    options.live = true;
  }

  hbase.getMetric(options, function(err, resp){
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
        message: 'error getting data'
      }).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {

    // csv
    if (options.format === 'csv') {
      resp.rows.forEach(function(r, i) {
        resp.rows[i] = utils.flattenJSON(r);
      });
      res.csv(resp.rows, metric + '.csv');

    // json
    } else {
      response.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        rows: resp.rows
      }).pipe(res);
    }
  }
}

module.exports = function(db) {
  hbase = db;
  return getMetric;
};
