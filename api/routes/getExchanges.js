'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'get payments'});
var moment = require('moment');
var response = require('response');
var intervals = [
  '1minute',
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day',
  '3day',
  '7day',
  '1month',
  '1year'
];
var hbase;

var getExchanges = function(req, res) {
  var options;

  function prepareOptions() {
    var options = {
      start: req.query.start,
      end: req.query.end,
      interval: req.query.interval,
      limit: Number(req.query.limit) || 200,
      base: {},
      counter: {},
      descending: (/false/i).test(req.query.descending) ? false : true,
      reduce: (/true/i).test(req.query.reduce) ? true : false,
      format: (req.query.format || 'json').toLowerCase()
    };

    var base = req.params.base.split(/[\+|\.]/); //any of +, |, or .
    var counter = req.params.counter.split(/[\+|\.]/);

    options.base.currency = base[0] ? base[0].toUpperCase() : undefined;
    options.base.issuer = base[1] ? base[1] : undefined;

    options.counter.currency = counter[0] ? counter[0].toUpperCase() : undefined;
    options.counter.issuer = counter[1] ? counter[1] : undefined;

    if (!options.base.currency) {
      return {error:'base currency is required', code:400};
    } else if (!options.counter.currency) {
      return {error:'counter currency is required', code:400};
    } else if (options.base.currency === 'XRP' && options.base.issuer) {
      return {error:'XRP cannot have an issuer', code:400};
    } else if (options.counter.currency === 'XRP' && options.counter.issuer) {
      return {error:'XRP cannot have an issuer', code:400};
    } else if (options.base.currency !== 'XRP' && !options.base.issuer) {
      return {error:'base issuer is required', code:400};
    } else if (options.counter.currency !== 'XRP' && !options.counter.issuer) {
      return {error:'counter issuer is required', code:400};
    }

    if (!options.end) {
      options.end = moment.utc('9999-12-31');
    }
    if (!options.start) {
      options.start = moment.utc(0);
    }
    if (options.interval) {
      options.interval = options.interval.toLowerCase();
    }
    if (options.interval === 'week') {
      options.interval = '7day';
    }

    if (options.reduce && options.limit > 20000) {
      options.limit = 20000;
    } else if (options.limit > 1000) {
      return {error: 'limit cannot exceed 1000', code: 400};
    } else if (options.interval &&
               intervals.indexOf(options.interval) === -1) {
      return {error: 'invalid interval: ' + options.interval, code: 400};
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
      response.json({result: 'error', message: 'unable to retrieve exchanges'})
      .status(500).pipe(res);
    }
  }

  /**
   * successResponse
   * return a successful response
   * @param {Object} exchanges
   */

  function successResponse(exchanges) {
    if (options.format === 'csv') {
      res.csv(exchanges, 'exchanges.csv');
    } else {
      response.json({
        result: 'sucess',
        count: exchanges.length,
        exchanges: exchanges
      }).pipe(res);
    }
  }

  options = prepareOptions();

  if (options.error) {
    errorResponse(options);

  } else {
    log.info('EXCHANGES: ' + options.base.currency, options.counter.currency);

    hbase.getExchanges(options, function(err, exchanges) {
      if (err) {
        errorResponse(err);
      } else if (options.reduce) {
        successResponse([exchanges]);
      } else {

        if (options.interval) {
          exchanges.forEach(function(ex) {
            delete ex.rowkey;
            delete ex.sort_open;
            delete ex.sort_close;
          });

        } else {
          exchanges.forEach(function(ex) {
            delete ex.rowkey;
            delete ex.node_index;
            delete ex.tx_index;
            delete ex.time;
            delete ex.client;

            ex.executed_time = moment.unix(ex.executed_time).utc().format();
          });
        }

        successResponse(exchanges);
      }
    });
  }
};


module.exports = function(db) {
  hbase = db;
  return getExchanges;
};
