var config   = require('../../storm/multilang/resources/config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'get payments'});
var moment   = require('moment');
var response = require('response');

var exchanges = function(hbase) {
  self = this;

  self.getExchanges = function (req, res, next) {
    var options = prepareOptions();

    if (options.error) {
      errorResponse(options);
      return;

    } else {
      log.info("EXCHANGES: " + options.base.currency, options.counter.currency);

      hbase.getExchanges(options, function(err, exchanges) {
        if (err) {
          errorResponse(err);
        } else if (options.reduce) {
          successResponse([exchanges]);
        } else if (!exchanges.length) {
          errorResponse({error: "no exchanges found", code: 404});
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

    function prepareOptions() {
      var options = {
        start      : req.query.start,
        end        : req.query.end,
        interval   : req.query.interval,
        limit      : Number(req.query.limit) || 200,
        base       : {},
        counter    : {},
        descending : (/false/i).test(req.query.descending) ? false : true,
        reduce     : (/true/i).test(req.query.reduce) ? true : false,
        interval   : req.query.interval,
        reduce     : req.query.reduce,
      }

      var base    = req.params.base.split(/[\+|\.]/); //any of +, |, or .
      var counter = req.params.counter.split(/[\+|\.]/);

      options.base.currency = base[0] ? base[0].toUpperCase() : undefined;
      options.base.issuer   = base[1] ? base[1] : undefined;

      options.counter.currency = counter[0] ? counter[0].toUpperCase() : undefined;
      options.counter.issuer   = counter[1] ? counter[1] : undefined;

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

      if (!options.end)   options.end   = moment.utc('9999-12-31');
      if (!options.start) options.start = moment.utc(0);

      if (options.reduce && options.limit > 20000) {
        options.limit = 20000;
      } else if (options.limit > 1000) {
        return {error:'limit cannot exceed 1000', code:400};
      }
      return options;
    }

    /**
    * errorResponse
    * return an error response
    * @param {Object} err
    */
    function errorResponse (err) {
      console.log(err);
      if (err.code.toString()[0] === '4') {
        log.error(err.error || err);
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
    function successResponse (exchanges) {
      var result = {
        result   : "sucess",
        count    : exchanges.length,
        exchanges : exchanges
      };

      response.json(result).pipe(res);
    }

  };

  return this;
}

module.exports = function(db) {
  ex = exchanges(db);
  return ex.getExchanges;
};
