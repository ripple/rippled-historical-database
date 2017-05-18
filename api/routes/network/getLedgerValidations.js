'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope : 'ledger validations'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase = require('../../../lib/hbase')

var getLedgerValidations = function (req, res, next) {

  var hexMatch = new RegExp('^(0x)?[0-9A-Fa-f]+$');
  var options = {
    ledger_hash: req.params.ledger_hash,
    pubkey: req.params.validation_pubkey,
    marker: req.query.marker,
    limit: Number(req.query.limit || 200),
    format: (req.query.format || 'json').toLowerCase()
  };


  // ledger hash test
  if (hexMatch.test(options.ledger_hash) && options.ledger_hash.length % 2 === 0) {
    options.ledger_hash = options.ledger_hash.toUpperCase();

  } else {
    errorResponse({
      error: "invalid ledger hash",
      code: 400
    });
    return;
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }


  log.info(options.ledger_hash);

  if (options.pubkey) {
    hbase.getScan({
      table: 'validations_by_ledger',
      startRow: options.ledger_hash + '|' + options.pubkey,
      stopRow: options.ledger_hash + '|' + options.pubkey + '~',
      limit: 1
    }, function(err, resp) {
      if (err) {
        errorResponse(err);

      } else if (resp && resp.length) {
        var validation = resp[0];
        validation.result = 'success';
        validation.count = Number(validation.count);
        delete validation.rowkey;

        res.json(validation);
      } else {
        errorResponse({
          error: "validation not found",
          code: 404
        });
      }
    });
    return;
  }

  hbase.getLedgerValidations(options)
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp);
    }
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
        message: 'unable to retrieve ledger'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} ledger
  */

  function successResponse(validations) {
    var filename = options.ledger_hash + ' - validations';

    if (validations.marker) {
      utils.addLinkHeader(req, res, validations.marker);
    }

    if (options.format === 'csv') {
      res.csv(validations.rows, filename + '.csv');

    } else {
      res.json({
        result: 'success',
        ledger_hash: options.ledger_hash,
        count: validations.rows.length,
        marker: validations.marker,
        validations: validations.rows
      });
    }
  }

};

module.exports = getLedgerValidations
