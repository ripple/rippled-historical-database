var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'get payments'});
var moment   = require('moment');
var response = require('response');
var hbase;

AccountExchanges = function (req, res, next) {

  var options = prepareOptions();

  hbase.getAccountExchanges(options, function(err, exchanges) {
    if (err) {
      errorResponse(err);

    } else {
      exchanges.rows.forEach(function(ex) {
        ex.executed_time = moment.unix(ex.executed_time).utc().format();

        delete ex.rowkey;
        delete ex.node_index;
        delete ex.tx_index;
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
      descending   : (/false/i).test(req.query.descending) ? false : true,
      start        : req.query.start,
      end          : req.query.end,
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

    if (!options.end)   options.end   = moment.utc('9999-12-31');
    if (!options.start) options.start = moment.utc(0);

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
      response.json({result:'error', message:err.error}).status(err.code).pipe(res);
    } else {
      response.json({result:'error', message:'unable to retrieve exchanges'}).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} exchanges
  */

  function successResponse(exchanges) {
    var filename = options.account + ' - exchanges';

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
      res.csv(exchanges.rows, filename);

    } else {
      response.json({
        result: 'success',
        count: exchanges.rows.length,
        marker: exchanges.marker,
        exchanges: exchanges.rows
      }).pipe(res);
    }
  }

};

module.exports = function(db) {
  hbase = db;
  return AccountExchanges;
};
