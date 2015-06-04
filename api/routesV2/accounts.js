'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'accounts'});
var smoment = require('../../lib/smoment');
var response = require('response');
var hbase;
var intervals = ['hour', 'day', 'week'];

/**
 * Accounts
 */

var Accounts = function (req, res, next) {
  var options;

  // prepareOptions
  function prepareOptions() {
    var opts = {
      start: smoment(req.query.start || 0),
      end: smoment(req.query.end),
      marker: req.query.marker,
      interval: req.query.interval,
      limit: Number(req.query.limit) || 200,
      descending: (/true/i).test(req.query.descending) ? true : false,
      reduce: (/true/i).test(req.query.reduce) ? true : false,
      parent: req.query.parent,
      format: (req.query.format || 'json').toLowerCase()
    };

    if (opts.interval && intervals.indexOf(opts.interval) === -1) {
      return {error: 'invalid interval', code: 400};
    } else if (opts.interval && opts.reduce) {
      return {error: 'cannot use reduce with interval', code: 400};
    } else if (opts.limit > 1000) {
      opts.limit = 1000;
    }
    return opts;
  }

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
        message: 'unable to get accounts'
      }).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {

    // reduced, csv
    if (options.reduce && options.format === 'csv') {
      res.csv([{count: resp.rows[0]}], 'accounts-count.csv');

    // reduced json
    } else if (options.reduce) {
      response.json({
        result: 'success',
        count: resp.rows[0]
      }).pipe(res);

    // csv
    } else if (options.format === 'csv') {
      res.csv(resp.rows, 'accounts.csv');

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


  options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;
  }

  hbase.getAccounts(options, function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp);
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return Accounts;
};

