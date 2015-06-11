'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'get account balance changes'});
var smoment = require('../../lib/smoment');
var response = require('response');
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
      return {error: 'invalid start time, must be ISO_8601', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end time, must be ISO_8601', code: 400};
    }

    if (options.issuer &&
       options.currency &&
       options.currency.toUpperCase() === 'XRP') {
      return {
        error: 'invalid request: an issuer cannot be specified for XRP',
        code: 400
      };
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
      response.json({result: 'error', message: err.error})
        .status(err.code).pipe(res);
    } else {
      response.json({
        result: 'error',
        message: 'unable to retrieve balance changes'
      }).status(500).pipe(res);
    }
  }

  /**
   * successResponse
   * return a successful response
   * @param {Object} balance changes
   */

  function successResponse(changes) {
    var filename = options.account + ' - balance changes';
    if (options.format === 'csv') {
      if (options.currency) {
        filename += ' ' + options.currency;
      }

      res.csv(changes.rows, filename + '.csv');
    } else {
      response.json({
        result: 'success',
        count: changes.rows.length,
        marker: changes.marker,
        balance_changes: changes.rows
      }).pipe(res);
    }
  }

};

module.exports = function(db) {
  hbase = db;
  return AcccountBalanceChanges;
};
