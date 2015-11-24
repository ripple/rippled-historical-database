'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'get account balance changes'});
var smoment = require('../../lib/smoment');
var utils = require('../../lib/utils');
var hbase;

/**
 * AccountBalanceChanges
 */

var AcccountBalanceChanges = function(req, res) {
  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;
  }

  log.info("ACCOUNT BALANCE CHANGE:", options.account);

  hbase.getAccountBalanceChanges(options, function(err, changes) {
    if (err) {
      errorResponse(err);

    } else {
      changes.rows.forEach(function(ex) {
        delete ex.rowkey;
        delete ex.client;
        delete ex.account;
        ex.executed_time = smoment(ex.executed_time).format();
      });

      successResponse(changes);
    }
  });

  function prepareOptions() {

    var options = {
      account: req.params.address,
      currency: req.query.currency,
      issuer: req.query.issuer,
      limit: req.query.limit,
      start: smoment(req.query.start || '2013-01-01'),
      end: smoment(req.query.end),
      marker: req.query.marker,
      descending: (/true/i).test(req.query.descending) ? true : false,
      format: (req.query.format || 'json').toLowerCase()
    }

    if (!options.start) {
      return {error: 'invalid start date format', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end date format', code: 400};
    }

    if (options.issuer &&
       options.currency &&
       options.currency.toUpperCase() === 'XRP') {
      return {
        error: 'invalid request: an issuer cannot be specified for XRP',
        code: 400
      };
    }

    if (isNaN(options.limit)) {
      options.limit = 200;

    } else if (options.limit > 1000) {
      options.limit = 1000;
    }

    return options;
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
        message: 'unable to retrieve balance changes'
      });
    }
  }

  /**
   * successResponse
   * return a successful response
   * @param {Object} balance changes
   */

  function successResponse(changes) {
    var filename = options.account + ' - balance changes';

    if (changes.marker) {
      utils.addLinkHeader(req, res, changes.marker);
    }

    if (options.format === 'csv') {
      if (options.currency) {
        filename += ' ' + options.currency;
      }

      res.csv(changes.rows, filename + '.csv');

    } else {
      res.json({
        result: 'success',
        count: changes.rows.length,
        marker: changes.marker,
        balance_changes: changes.rows
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return AcccountBalanceChanges;
};
