'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'network fees'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase = require('../../../lib/hbase')
var intervals = [
  'ledger',
  'hour',
  'day'
];

var getFees = function(req, res) {
  var options = {
    interval: req.query.interval || 'ledger',
    start: smoment(req.query.start || '2013-01-01'),
    end: smoment(req.query.end),
    limit: req.query.limit,
    marker: req.query.marker,
    descending: (/true/i).test(req.query.descending) ? true : false,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (!options.start) {
    errorResponse({
      error: 'invalid start date format',
      code: 400
    });
    return;

  } else if (!options.end) {
    errorResponse({
      error: 'invalid end date format',
      code: 400
    });
    return;

  } else if (intervals.indexOf(options.interval) === -1) {
    errorResponse({
      error: 'invalid interval',
      code: 400
    });
    return;
  }

  if (isNaN(options.limit)) {
      options.limit = 200;
  } else if (options.limit > 1000) {
      options.limit = 1000;
  }

  log.info('interval:', options.interval);

  hbase.getNetworkFees(options)
  .then(successResponse)
  .catch(errorResponse);

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
        message: 'unable to retrieve fee summary(s)'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(data) {
    var filename;

    if (data.marker) {
      utils.addLinkHeader(req, res, data.marker);
    }

    if (options.format === 'csv') {
      filename = 'network fees.csv';
      res.csv(data.rows, filename);

    } else {
      res.json({
        result: 'success',
        marker: data.marker,
        count: data.rows.length,
        rows: data.rows
      });
    }
  }
};

module.exports = getFees
