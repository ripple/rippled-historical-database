var response = require('response');
var Logger = require('../../lib/logger');
var log = new Logger({scope : 'health check'});
var moment = require('moment');
var hbase;

/*
    /v2/health/ (defaults to api)
    /v2/health/api
    /v2/health/importer
    /v2/health/importer?verbose=true
    /v2/health/importer?verbose=true&threshold=120&threshold2=10

    default response is the integer score only, and 0 is good health
    'verbose' parameter will result in a JSON response with more details

    API health:
      0 hbase response time < 5 seconds
      1 hbase response time greater than 5 seconds
      2 hbase response error or invalid response time

      `threshold` parameter corresponds to response time,
      measured in seconds, defaults to 5

    Importer health:

      0 last ledger imported < 60 seconds ago && last validated ledger < 300 seconds ago
      1 last ledger imported < 60 seconds ago
      2 last ledger imported over 60 seconds ago
      3 hbase response error or invalid response time

      `threshold` parameter corresponds to the ledger validator gap, measured in seconds, defaults to 300
      'threshold2' parameter corresponds to the ledger close time gap, measured in seconds, defaults to 60
*/

var checkHealth = function(req, res) {
  var _DEFAULT_API_THRESHOLD1 = 5; // response time
  var _DEFAULT_API_THRESHOLD2 = 15; // unused
  var _DEFAULT_IMPORTER_THRESHOLD1 = 60 * 5; // importer
  var _DEFAULT_IMPORTER_THRESHOLD2 = 60 * 15;  // validator

  var aspect = (req.params.aspect || 'api').toLowerCase();
  var verbose = (/true/i).test(req.query.verbose) ? true : false;
  var t1 = Number(req.query.threshold || (aspect === 'api' ?
    _DEFAULT_API_THRESHOLD1 : _DEFAULT_IMPORTER_THRESHOLD1));
  var t2 = Number(req.query.threshold2 || (aspect === 'api' ?
    _DEFAULT_API_THRESHOLD2 : _DEFAULT_IMPORTER_THRESHOLD2));

  var d = Date.now();

  if (aspect !== 'api' && aspect !== 'importer') {
    response.json({result: 'error', message: 'invalid aspect type'})
    .status(400).pipe(res);
    return;
  }

  if (isNaN(t1) || isNaN(t2)) {
    response.json({
      result: 'error',
      message: 'invalid threshold'
    }).status(400).pipe(res);
    return;
  }

  log.info(aspect);

  hbase.getLedger({}, function(err, ledger) {
    var now = Date.now();
    var gap = ledger ? (now - ledger.close_time * 1000)/1000 : Infinity;
    var responseTime = (Date.now() - d) / 1000;

    if (aspect === 'api') {
      apiHealthResponse(responseTime, err, verbose);
    } else {
      importerHealthResponse(responseTime, gap, err, verbose);
    }
  });

  /**
   * apiHealthResponse
   */

  function apiHealthResponse(responseTime, err, verbose) {
    var score;
    var message;

    if (err) {
      score = 2;
      message = 'hbase response error';
    } else if (responseTime < 0 || isNaN(responseTime)) {
      score = 2;
      message = 'invalid response time';
    } else if (responseTime > t1) {
      score = 1;
      message = 'response time exceeds threshold';
    } else {
      score = 0;
    }

    if (verbose) {
      response.json({
        score: score,
        response_time: duration(responseTime * 1000),
        response_time_threshold: duration(t1 * 1000),
        message: message,
        error: err || undefined
      }).pipe(res);
    } else {
      res.send(score.toString());
    }
  }

  /**
   * importerHealthResponse
   */

  function importerHealthResponse(responseTime, ledgerGap, err, verbose) {

    // get last validated ledger
    hbase.getLastValidated(function(err, resp) {
      var now = Date.now();
      var closeTime = moment.utc(resp.close_time).unix() * 1000;
      var validatorGap = (now - closeTime)/1000;
      var score;
      var message;

      if (err) {
        score = 3;
        message = 'hbase response error';
      } else if (responseTime < 0 || isNaN(responseTime)) {
        score = 3;
        message = 'invalid response time';
      } else if (ledgerGap > t1) {
        score = 2;
        message = 'last ledger gap exceeds threshold';
      } else if (validatorGap > t2) {
        score = 1;
        message = 'last validation gap exceeds threshold';
      } else {
        score = 0;
      }

      if (verbose) {
        response.json({
          score: score,
          response_time: duration(responseTime * 1000),
          ledger_gap: duration(ledgerGap * 1000),
          ledger_gap_threshold: duration(t1 * 1000),
          validation_gap: duration(validatorGap * 1000),
          validation_gap_threshold: duration(t2 * 1000),
          message: message,
          error: err || undefined
        }).pipe(res);
      } else {
        res.send(score.toString());
      }
    });
  }
};

// function for formatting duration
function duration (ms) {

    if (ms === Infinity) {
      return ms.toString();
    }

    var s = Math.floor(ms / 1000);
    var years = Math.floor(s / 31536000);
    if (years) {
      return (s / 31536000).toFixed(2) + 'y';
    }

    var days = Math.floor((s %= 31536000) / 86400);
    if (days) {
      return ((s %= 31536000) / 86400).toFixed(2) + 'd';
    }

    var hours = Math.floor((s %= 86400) / 3600);
    if (hours) {
      return ((s %= 86400) / 3600).toFixed(2) + 'h';
    }

    var minutes = Math.floor((s %= 3600) / 60);
    if (minutes) {
      return ((s %= 3600) / 60).toFixed(2) + 'm';
    }

    return ms/1000 + 's';
}

module.exports = function(db) {
  hbase = db;
  return checkHealth;
};
