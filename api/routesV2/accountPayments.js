'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account payments'});
var smoment = require('../../lib/smoment');
var response = require('response');
var utils = require('../../lib/utils');
var types = ['sent', 'received'];
var hbase;

/**
 * AccountPayments
 */

var AccountPayments = function (req, res, next) {
  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;

  } else {
    log.info("get: " + options.account);

    hbase.getAccountPayments(options, function(err, payments) {
      if (err) {
        errorResponse(err);
      } else {
        payments.rows.forEach(function(p) {
          delete p.rowkey;
          delete p.tx_index;
          delete p.client;
          p.executed_time = smoment(p.executed_time).format();
        });

        successResponse(payments);
      }
    });
  }

  /**
   * prepareOptions
   */

  function prepareOptions() {
    var options = {
      account: req.params.address,
      start: smoment(req.query.start || '2013-01-01'),
      end: smoment(req.query.end),
      type: req.query.type ? req.query.type.toLowerCase() : undefined,
      currency: req.query.currency ? req.query.currency.toUpperCase() : undefined,
      marker: req.query.marker,
      descending: (/true/i).test(req.query.descending) ? true : false,
      limit: Number(req.query.limit) || 200,
      format: (req.query.format || 'json').toLowerCase()
    };

    if (!options.start) {
      return {error: 'invalid start time format', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end time format', code: 400};
    }

    if (!options.account) {
      return {error: 'Account is required', code: 400};
    } else if (options.type && types.indexOf(options.type) === -1) {
      return {error: 'invalid type - use: ' + types.join(', '), code: 400};
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
      response.json({result: 'error', message: err.error})
        .status(err.code).pipe(res);
    } else {
      response.json({result: 'error', message: 'unable to retrieve payments'})
        .status(500).pipe(res);
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(payments) {
    var filename = options.account + ' - payments';
    var results = [ ];

    if (options.format === 'csv') {
      if (options.type) {
        filename += ' ' + options.type;
      }
      if (options.currency) {
        filename += ' ' + options.currency;
      }

      payments.rows.forEach(function(r) {
        results.push(utils.flattenJSON(r));
      });

      res.csv(results, filename + '.csv');
    } else {
      response.json({
        result: 'success',
        count: payments.rows.length,
        marker: payments.marker,
        payments: payments.rows
      }).pipe(res);
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return AccountPayments;
};
