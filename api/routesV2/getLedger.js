'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'get ledger'});
var smoment = require('../../lib/smoment');
var hbase;

var getLedger = function (req, res, next) {

  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
  } else {

    if (options.ledger_index) {
      log.info('LEDGER:', options.ledger_index);
    } else if (options.ledger_hash) {
      log.info('LEDGER:', options.ledger_hash);
    } else if (options.closeTime) {
      log.info('LEDGER:', options.closeTime.format());
    } else {
      log.info('LEDGER: latest');
    }

    hbase.getLedger(options, function(err, ledger) {
      if (err) {
        errorResponse(err);
      } else if (!ledger) {
        errorResponse({error: "ledger not found", code: 404});
      } else {
        successResponse(ledger);
      }
    });
  }
   /**
  * prepareOptions
  * parse request parameters to determine query options
  */

  function prepareOptions() {
    var options = {
      ledger_index: req.query.ledger_index,
      ledger_hash: req.query.ledger_hash,
      binary: (/true/i).test(req.query.binary) ? true : false,
      expand: (/true/i).test(req.query.expand) ? true : false,
      transactions: (/true/i).test(req.query.transactions) ? true : false
    };

    var ledger_param = req.params.ledger_param;
    var intMatch = /^\d+$/;
    var hexMatch = new RegExp('^(0x)?[0-9A-Fa-f]+$');
    var date = smoment(ledger_param);

    if (ledger_param) {

      // ledger index test
      if (intMatch.test(ledger_param)) {
        options.ledger_index = ledger_param;

      // date test
      } else if (date) {
        options.closeTime = date;

      // ledger hash test
      } else if (hexMatch.test(ledger_param) && ledger_param.length % 2 === 0) {
        options.ledger_hash = ledger_param.toUpperCase();

      } else {
        return {
          error: "invalid ledger identifier",
          code: 400
        };
      }

    } else if (req.query.date) {
      date = smoment(req.query.date);

      if (date) {
        options.closeTime = date;

      } else {
        return {
          error: 'invalid date format',
          code: 400
        };
      }
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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve ledger'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} ledger
  */

  function successResponse(ledger) {
    res.json({
      result: 'success',
      ledger: ledger
    });
  }

};

module.exports = function(db) {
  hbase = db;
  return getLedger;
};
