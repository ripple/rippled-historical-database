'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'capitalization'});
var smoment = require('../../lib/smoment');
var response = require('response');
var intervals = ['day', 'week', 'month'];
var validator = require('ripple-address-codec');
var hbase;

var getCapitalization = function (req, res, next) {

  var options = {
    currency: req.params.currency,
    issuer: req.params.issuer,
    start: smoment(req.query.start || 0),
    end: smoment(req.query.end),
    interval: req.query.interval,
    adjusted: (/true/i).test(req.query.adjusted) ? true : false,
    descending: (/true/i).test(req.query.descending) ? true : false,
    limit: req.query.limit || 200,
    marker: req.query.marker,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (!validator.isValidAddress(options.issuer)) {
    errorResponse({error: 'invalid issuer address', code: 400});
    return;

  } else if (!options.start) {
    errorResponse({error: 'invalid start date format', code: 400});
    return;

  } else if (!options.end) {
    errorResponse({error: 'invalid end date format', code: 400});
    return;

  } else if (options.interval &&
             intervals.indexOf(options.interval) === -1) {
    errorResponse({
      error: 'invalid interval - use: '+intervals.join(', '),
      code: 400
    });
    return;
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

    hbase.getCapitalization(options, function(err, resp) {
      if (err || !resp) {
        errorResponse(err);
        return;
      }

      resp.rows.forEach(function(r) {
        r.amount = r.amount.toString();
      });

      resp.currency = options.currency;
      resp.issuer = options.issuer;
      successResponse(resp);
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
      response.json({result: 'error', message: 'unable to retrieve data'})
        .status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {
    var filename;

    if (options.format === 'csv') {
      filename = 'capitalization - ' +
        resp.currency + ' ' +
        resp.issuer + '.csv';
      res.csv(resp.rows, filename);

    // json
    } else {
      response.json({
        result: 'success',
        currency: resp.currency,
        issuer: resp.issuer,
        count: resp.rows.length,
        marker: resp.marker,
        rows: resp.rows
      }).pipe(res);
    }
  }

};

module.exports = function(db) {
  hbase = db;
  return getCapitalization;
};
