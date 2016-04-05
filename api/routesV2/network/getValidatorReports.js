'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'validator reports'});
var smoment = require('../../../lib/smoment');
var hbase;

var getValidatorReports = function(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    date: req.query.date ? smoment(req.query.date) : undefined,
    format: (req.query.format || 'json').toLowerCase()
  };
  var days;

  if (options.pubkey) {
    options.start = smoment(req.query.start);
    options.end = smoment(req.query.end);

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

    } else if (!req.query.start) {
      options.start.moment.subtract(200, 'days');
    }

    days = options.end.moment.diff(options.start.moment, 'days');
    if (!days) {
      options.start.moment.startOf('day');

    } else if(Math.abs(days) > 200) {
      errorResponse({
        error: 'choose a date range less than 200 days',
        code: 400
      });
      return;
    }

    log.info(options.pubkey);

  } else {
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
  }

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
      res.csv(data.rows, filename);

    } else {
      res.json({
        result: 'success',
        count: data.rows.length,
        reports: data.rows
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return getValidatorReports;
};
