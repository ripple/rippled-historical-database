'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'accounts'});
var smoment = require('../../lib/smoment');
var utils = require('../../lib/utils');
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
      start: smoment(req.query.start || '2013-01-01'),
      end: smoment(req.query.end),
      marker: req.query.marker,
      interval: req.query.interval,
      limit: Number(req.query.limit || 200),
      descending: (/true/i).test(req.query.descending) ? true : false,
      reduce: (/true/i).test(req.query.reduce) ? true : false,
      parent: req.query.parent,
      format: (req.query.format || 'json').toLowerCase()
    };

    if (!opts.start) {
      return {error: 'invalid start date format', code: 400};
    } else if (!opts.end) {
      return {error: 'invalid end date format', code: 400};
    }


    if (opts.interval && intervals.indexOf(opts.interval) === -1) {
      return {error: 'invalid interval', code: 400};
    } else if (opts.interval && opts.reduce) {
      return {error: 'cannot use reduce with interval', code: 400};
    } else if (opts.interval && opts.parent) {
      return {error: 'cannot use parent with interval', code: 400};
    }

    if (isNaN(opts.limit)) {
      opts.limit = 200;

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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to get accounts'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {

    if (resp.marker) {
      utils.addLinkHeader(req, res, resp.marker);
    }

    // reduced, csv
    if (options.reduce && options.format === 'csv') {
      res.csv([{count: resp}], 'accounts-count.csv');

    // reduced json
    } else if (options.reduce) {
      res.json({
        result: 'success',
        count: resp
      });

    // csv
    } else if (options.format === 'csv') {
      res.csv(resp.rows, 'accounts.csv');

    // json
    } else {
      res.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        accounts: resp.rows
      });
    }
  }


  options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;
  }

  hbase.getAccounts(options, function(err, resp) {
    var accounts = [];
    if (err || !resp) {
      errorResponse(err);
    } else if (options.reduce) {
      successResponse(resp.rows[0]);
    } else if (options.interval) {
      successResponse(resp);
    } else {
      resp.rows.forEach(function(row) {
        accounts.push({
          account: row.account,
          parent: row.parent,
          initial_balance: row.balance,
          inception: smoment(row.executed_time).format(),
          ledger_index: row.ledger_index,
          tx_hash: row.tx_hash,
          genesis_balance: row.genesis_balance,
          genesis_index: row.genesis_index
        });
      });
      resp.rows = accounts;
      successResponse(resp);
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return Accounts;
};

