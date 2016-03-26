'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'validator reports'});
var smoment = require('../../../lib/smoment');
var hbase;

var getValidatorReports = function(req, res) {
  var options = {
    date: req.query.date ? smoment(req.query.date) : undefined,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.query.date) {
    options.start = smoment(req.query.date);

    if (!options.start) {
      errorResponse({
        error: 'invalid date format',
        code: 400
      });
      return;
    }

    options.start.moment.startOf('day');
    options.end = smoment(options.start);
  }

  log.info(options.start ? options.start.format() : 'latest')

  hbase.getValidatorReports(options)
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp, options);
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
        message: 'unable to retrieve validator reports'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(data, options) {
    var filename;

    if (options.format === 'csv') {
      filename = 'validator reports.csv';
      res.csv(data.reports, filename);

    } else {
      res.json({
        result: 'success',
        count: data.reports.length,
        reports: data.reports
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return getValidatorReports;
};
