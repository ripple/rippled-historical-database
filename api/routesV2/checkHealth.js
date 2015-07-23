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
*/

var checkHealth = function(req, res) {
  var aspect = (req.params.aspect || 'api').toLowerCase();
  var verbose = (/true/i).test(req.query.verbose) ? true : false;
  var t1 = Number(req.query.threshold || (aspect === 'api' ? 5 : 300));
  var t2 = Number(req.query.threshold2 || (aspect === 'api' ? 15 : 60));

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
    var now = moment.utc().unix();
    var gap = ledger ? now - ledger.close_time : Infinity;
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
      message = 'response time exceeds ' + t1 + 's';
    } else {
      score = 0;
    }

    if (verbose) {
      response.json({
        score: score,
        responseTime: responseTime + 's',
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

  function importerHealthResponse(responseTime, gap, err, verbose) {
    var score;
    var message;

    if (err) {
      score = 3;
      message = 'hbase response error';
    } else if (responseTime < 0 || isNaN(responseTime)) {
      score = 3;
      message = 'invalid response time';
    } else if (gap > t2) {
      score = 2;
      message = 'last ledger more than ' + t2 + ' seconds ago';
    } else {
      hbase.getLastValidated(function(err, resp) {
        var now = moment.utc().unix();
        var closeTime = moment.utc(resp.close_time).unix();
        var validatorGap = now - closeTime;

        if (err) {
          score = 3;
          message = 'hbase response error';
        } else if (validatorGap > t1) {
          score = 1;
          message = 'last validated ledger more than ' + t1 + ' seconds ago';
        } else {
          score = 0;
        }

        if (verbose) {
          response.json({
            score: score,
            responseTime: responseTime + 's',
            ledgerGap: gap + 's',
            validatorGap: validatorGap + 's',
            message: message,
            error: err || undefined
          }).pipe(res);
        } else {
          res.send(score.toString());
        }
      });
      return;
    }

    if (verbose) {
      response.json({
        score: score,
        responseTime: responseTime + 's',
        ledgerGap: gap + 's',
        message: message,
        error: err || undefined
      }).pipe(res);
    } else {
      res.send(score.toString());
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return checkHealth;
};
