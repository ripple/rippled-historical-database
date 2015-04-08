'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'accounts'});
var moment = require('moment');
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
      start: moment.utc(req.query.start || 0),
      end: moment.utc(req.query.end),
      marker: req.query.marker,
      interval: req.query.interval,
      limit: Number(req.query.limit) || 200,
      descending: (/false/i).test(req.query.descending) ? false : true,
      reduce: (/true/i).test(req.query.reduce) ? true : false,
      parent: req.query.parent
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

  function successResponse(resp, reduced) {
    var result;
    if (reduced) {
      result = {
        result: 'success',
        count: resp.rows[0]
      };
    } else {
      result = {
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        rows: resp.rows
      };
    }

    response.json(result).pipe(res);
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
      successResponse(resp, options.reduce);
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return Accounts;
};

