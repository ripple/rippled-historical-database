var config   = require('../../storm/multilang/resources/config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'get payments'});
var moment   = require('moment');
var response = require('response');

var exchanges = function(hbase) {
  self = this;

self.getExchanges = function (req, res, next) {
  var options = prepareOptions();
  
  if (options.error) errorResponse(options.error);
  else {
    log.info("EXCHANGES: " + options.base.currency, options.counter.currency);
    
    hbase.getExchanges(options, function(err, exchanges) {
      if (err) errorResponse(err);
      else if 
        (exchanges.length === 0) errorResponse({error: "no exchanges found", code: 404});
      else successResponse(exchanges);
    });
  }

  function prepareOptions() {
    var options = {
      start      : req.query.start,
      end        : req.query.end,
      interval   : req.query.interval,
      limit      : req.query.limit || 20,
      base       : {},
      counter    : {},
      descending : !req.query.descending || req.query.descending === "false" ? false : true
    }

    var baseParams    = req.params.base.split("+");
    var counterParams = req.params.counter.split("+");

    if (baseParams[0] === "XRP") {
      options.base.currency = "XRP";
      //options.base.issuer   = "";
    }
    else if (baseParams[1]) {
      options.base.currency = baseParams[0];
      options.base.issuer   = baseParams[1];
    }
    else 
      options.error = {error:"enter valid base", code:400};

    if (counterParams[0] === "XRP") {
      options.counter.currency = "XRP";
      options.counter.issuer   = "";
    }
    else if (counterParams[1]) {
      options.counter.currency = counterParams[0];
      options.counter.issuer   = counterParams[1];
    }
    else
      options.error = {error:"enter valid counter", code:400};

    if (!options.start || !options.end) options.error = {error:"must provide start and end dates", code:400};
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