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
      exchanges.forEach(function(ex) {
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
      limit        : req.query.limit || 200,
      descending   : (/false/i).test(req.query.descending) ? false : true,
      start        : req.query.start,
      end          : req.query.end,
    };

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

module.exports = function(db) {
  hbase = db;
  return AccountExchanges;
};
