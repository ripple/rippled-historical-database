var Logger = require('../../lib/logger');
var log = new Logger({scope : 'account exchanges'});
var smoment = require('../../lib/smoment');
var utils = require('../../lib/utils');
var hbase;

AccountExchanges = function (req, res, next) {

  var options = prepareOptions();

  if (!options.start) {
    errorResponse({
      error: 'invalid start date format',
      code: 400
    });
    return;

  } else if (!options.end) {
    errorResponse({
      error: 'invalid end date format',
      code: 400
    });
    return;
  }

  hbase.getAccountExchanges(options, function(err, exchanges) {
    if (err) {
      errorResponse(err);

    } else {
      exchanges.rows.forEach(function(ex) {
        ex.executed_time = smoment(parseInt(ex.executed_time)).format();
        ex.base_amount = ex.base_amount.toString();
        ex.counter_amount = ex.counter_amount.toString();
        ex.rate = ex.rate.toPrecision(8);
        delete ex.rowkey;
      });

      successResponse(exchanges);
    }
  });

 /**
  * prepareOptions
  * parse request parameters to determine query options
  */

  function prepareOptions () {
    var options = {
      account      : req.params.address,
      base         : req.params.base,
      counter      : req.params.counter,
      limit        : req.query.limit || 200,
      marker       : req.query.marker,
      descending   : (/true/i).test(req.query.descending) ? true : false,
      start        : smoment(req.query.start || '2013-01-01'),
      end          : smoment(req.query.end),
      format       : (req.query.format || 'json').toLowerCase()
    };

    var base    = req.params.base ? req.params.base.split(/[\+|\.]/) : undefined;
    var counter = req.params.counter ? req.params.counter.split(/[\+|\.]/) : undefined;

    options.base= {};
    options.base.currency = base && base[0] ? base[0].toUpperCase() : undefined;
    options.base.issuer   = base && base[1] ? base[1] : undefined;

    options.counter= {};
    options.counter.currency = counter && counter[0] ? counter[0].toUpperCase() : undefined;
    options.counter.issuer   = counter && counter[1] ? counter[1] : undefined;

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

  function errorResponse (err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      res.status(err.code).json({
        result:'error',
        message:err.error
      });
    } else {
      res.status(500).json({
        result:'error',
        message:'unable to retrieve exchanges'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} exchanges
  */

  function successResponse(exchanges) {
    var filename = options.account + ' - exchanges';

    if (exchanges.marker) {
      utils.addLinkHeader(req, res, exchanges.marker);
    }

    if (options.format === 'csv') {
      if (options.base.currency && options.counter.currency) {
        filename += ' - ' +
          options.base.currency + '-' +
          options.counter.currency;
      } else if (options.base.currency) {
        filename += ' - ' + options.base.currency;
      } else if (options.counter.currency) {
        filename += ' - ' + options.counter.currency;
      }

      filename += '.csv';

      //ensure consistency in ordering
      if (exchanges.rows.length) {
        exchanges.rows[0] = {
          base_currency: exchanges.rows[0].base_currency,
          base_issuer: exchanges.rows[0].base_issuer,
          counter_currency: exchanges.rows[0].counter_currency,
          counter_issuer: exchanges.rows[0].counter_issuer,
          base_amount: exchanges.rows[0].base_amount,
          counter_amount: exchanges.rows[0].counter_amount,
          rate: exchanges.rows[0].rate,
          executed_time: exchanges.rows[0].executed_time,
          ledger_index: exchanges.rows[0].ledger_index,
          buyer: exchanges.rows[0].buyer,
          seller: exchanges.rows[0].seller,
          taker: exchanges.rows[0].taker,
          provider: exchanges.rows[0].provider,
          autobridged_currency: exchanges.rows[0].autobridged_currency,
          autobridged_issuer: exchanges.rows[0].autobridged_issuer,
          offer_sequence: exchanges.rows[0].offer_sequence,
          tx_type: exchanges.rows[0].tx_type,
          tx_index: exchanges.rows[0].tx_index,
          node_index: exchanges.rows[0].node_index,
          tx_hash: exchanges.rows[0].tx_hash
        };
      }

      res.csv(exchanges.rows, filename);

    } else {
      res.json({
        result: 'success',
        count: exchanges.rows.length,
        marker: exchanges.marker,
        exchanges: exchanges.rows
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return AccountExchanges;
};
