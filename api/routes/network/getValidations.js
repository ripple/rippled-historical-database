'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'get validations'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase = require('../../../lib/hbase')

var getValidations = function(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    start: smoment(req.query.start || '2013-01-01'),
    end: smoment(req.query.end),
    marker: req.query.marker,
    limit: Number(req.query.limit || 200),
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
  }

  var max = smoment()
  max.moment.subtract(3, 'months')

  if (options.start.moment.diff(max.moment) < 0) {
    options.start = max
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

  log.info(options.start.format(), options.end.format(), options.pubkey || '');

  hbase.getValidations(options)
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
        message: 'unable to retrieve validations'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(d) {
    var filename = options.pubkey ? options.pubkey + ' ' : '';

    if (d.marker) {
      utils.addLinkHeader(req, res, d.marker);
    }

    if (options.format === 'csv') {
      res.csv(d.rows, filename + 'validations.csv');

    } else {
      res.json({
        result: 'success',
        count: d.rows.length,
        marker: d.marker,
        validations: d.rows
      });
    }
  }
};

module.exports = getValidations
