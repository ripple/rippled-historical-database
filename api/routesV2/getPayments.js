'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'payments'});
var smoment = require('../../lib/smoment');
var utils = require('../../lib/utils');
var intervals = ['day', 'week', 'month'];
var validator = require('ripple-address-codec');
var hbase;

var getPayments = function (req, res, next) {

  var options = {
    start: smoment(req.query.start || 0),
    end: smoment(req.query.end),
    interval: req.query.interval,
    descending: (/true/i).test(req.query.descending) ? true : false,
    limit: Number(req.query.limit || 200),
    marker: req.query.marker,
    format: (req.query.format || 'json').toLowerCase()
  };

  var currency = req.params.currency;

  if (currency) {
    currency = currency.split(/[\+|\.]/);  // any of +, |, or .
    options.currency = currency[0].toUpperCase();
    options.issuer = currency[1];
  }

  if (options.issuer && !validator.isValidAddress(options.issuer)) {
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
    errorResponse({error: 'invalid interval', code: 400});
    return;

  } else if (options.currency &&
             options.currency !== 'XRP' &&
            !options.issuer) {
    errorResponse({error: 'issuer is required', code: 400});
    return;

  } else if (options.interval && !options.currency) {
    errorResponse({error: 'currency is required for aggregated payments', code: 400});
    return;
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

    hbase.getPayments(options, function(err, resp) {
      if (err || !resp) {
        errorResponse(err);
        return;
      }

      resp.rows.forEach(function(r) {
        delete r.rowkey;
        if (options.interval) {
          r.start = smoment(r.date).format();
          r.total_amount = r.amount.toString();
          r.average_amount = r.average.toString();
          delete r.date;
          delete r.amount;
          delete r.average;

          if (r.issuer === '') {
            delete r.issuer;
          }

        } else {
          r.executed_time = smoment(r.executed_time).format();
          r.transaction_cost = r.fee;
          delete r.fee;
          delete r.rowkey;
          delete r.client;
        }
      });

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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve payments'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {
    var filename;

    if (resp.marker) {
      utils.addLinkHeader(req, res, resp.marker);
    }

    if (options.format === 'csv') {
      filename = 'payments' +
        (resp.currency ? ' - ' + resp.currency : ' ') +
        (resp.issuer ? ' ' + resp.currency : ' ') + '.csv';
      resp.rows.forEach(function(r,i) {
        resp.rows[i] = utils.flattenJSON(r);
      });
      res.csv(resp.rows, filename);

    // json
    } else {
      res.json({
        result: 'success',
        currency: resp.currency,
        issuer: resp.issuer,
        count: resp.rows.length,
        marker: resp.marker,
        payments: resp.rows
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return getPayments;
};
