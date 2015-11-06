'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'exchanges'});
var smoment = require('../../lib/smoment');
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
      start: smoment(req.query.start || '2013-01-01'),
      end: smoment(req.query.end),
      interval: req.query.interval,
      limit: Number(req.query.limit || 200),
      base: {},
      counter: {},
      descending: (/true/i).test(req.query.descending) ? true : false,
      reduce: (/true/i).test(req.query.reduce) ? true : false,
      autobridged: (/true/i).test(req.query.autobridged) ? true : false,
      format: (req.query.format || 'json').toLowerCase(),
      marker: req.query.marker
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

    if (!options.start) {
      return {error: 'invalid start date format', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end date format', code: 400};
    }

    if (options.interval) {
      options.interval = options.interval.toLowerCase();
    }
    if (options.interval === 'week') {
      options.interval = '7day';
    }

    if (isNaN(options.limit)) {
      return {error: 'invalid limit: ' + options.limit, code: 400};
    } else if (options.reduce && options.interval) {
      return {error: 'cannot use reduce with interval', code: 400};
    } else if (options.reduce) {
      options.limit = req.query.limit ? options.limit : 50000;
    } else if (options.limit > 1000) {
      options.limit = 1000;
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

  function successResponse(resp) {
    var filename;

    if (options.format === 'csv') {
      filename = 'exchanges - ' +
        options.base.currency + '-' +
        options.counter.currency +
        '.csv';

      // ensure consistent order and
      // inclusion of all fields
      if (resp.rows.length &&
         (options.reduce || options.interval)) {

        resp.rows[0] = {
          open: resp.rows[0].open,
          high: resp.rows[0].high,
          low: resp.rows[0].low,
          close: resp.rows[0].close,
          vwap: resp.rows[0].vwap,
          count: resp.rows[0].count,
          base_currency: resp.rows[0].base_currency,
          base_issuer: resp.rows[0].base_issuer,
          base_volume: resp.rows[0].base_volume,
          counter_currency: resp.rows[0].counter_currency,
          counter_issuer: resp.rows[0].counter_issuer,
          counter_volume: resp.rows[0].counter_volume,
          open_time: resp.rows[0].open_time,
          close_time: resp.rows[0].close_time,
          start: resp.rows[0].start
        };

      } else if (resp.rows.length) {
        resp.rows[0] = {
          base_currency: resp.rows[0].base_currency,
          base_issuer: resp.rows[0].base_issuer,
          base_amount: resp.rows[0].base_amount,
          counter_amount: resp.rows[0].counter_amount,
          counter_currency: resp.rows[0].counter_currency,
          counter_issuer: resp.rows[0].counter_issuer,
          rate: resp.rows[0].rate,
          executed_time: resp.rows[0].executed_time,
          ledger_index: resp.rows[0].ledger_index,
          buyer: resp.rows[0].buyer,
          seller: resp.rows[0].seller,
          taker: resp.rows[0].taker,
          provider: resp.rows[0].provider,
          autobridged_currency: resp.rows[0].autobridged_currency,
          autobridged_issuer: resp.rows[0].autobridged_issuer,
          offer_sequence: resp.rows[0].offer_sequence,
          tx_type: resp.rows[0].tx_type,
          tx_index: resp.rows[0].tx_index,
          node_index: resp.rows[0].node_index,
          tx_hash: resp.rows[0].tx_hash
        };
      }

      res.csv(resp.rows, filename);
    } else {
      response.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        exchanges: resp.rows
      }).pipe(res);
    }
  }

  options = prepareOptions();

  if (options.error) {
    errorResponse(options);

  } else {
    log.info(options.base.currency, options.counter.currency);

    hbase.getExchanges(options, function(err, resp) {
      if (err) {
        errorResponse(err);
      } else if (options.reduce) {
        resp.rows = [resp.reduced];
        successResponse(resp);

      } else {
        if (options.interval) {
          resp.rows.forEach(function(ex) {
            delete ex.rowkey;
            delete ex.sort_open;
            delete ex.sort_close;

            ex.open_time = smoment(ex.open_time).format();
            ex.close_time = smoment(ex.close_time).format();
            ex.start = smoment(ex.start).format();
            ex.base_currency = options.base.currency;
            ex.base_issuer = options.base.issuer;
            ex.counter_currency = options.counter.currency;
            ex.counter_issuer = options.counter.issuer;
          });

        } else {
          resp.rows.forEach(function(ex) {
            delete ex.rowkey;
            delete ex.time;
            delete ex.client;

            ex.executed_time = smoment(ex.executed_time).format();
            ex.base_currency = options.base.currency;
            ex.base_issuer = options.base.issuer;
            ex.counter_currency = options.counter.currency;
            ex.counter_issuer = options.counter.issuer;
          });
        }

        successResponse(resp);
      }
    });
  }
};


module.exports = function(db) {
  hbase = db;
  return getExchanges;
};
