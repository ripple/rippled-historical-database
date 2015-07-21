var response = require('response');
var Logger = require('../../lib/logger');
var log = new Logger({scope : 'health check'});
var moment = require('moment');
var hbase;

/*
    6 best health
    5 last validated over 5 minutes ago
    4 last ledger over 60 seconds ago
    3 last ledger over 5 minutes ago
    2 hbase response time over 5 seconds
    1 hbase response time over 15 seconds
    0 hbase response timeout/error

    less than 2 is failing score
*/

var checkHealth = function(req, res) {
  var d = Date.now();

  log.info(moment.utc().format());

  hbase.getLedger({}, function(err, ledger) {
    var now = moment.utc().unix();
    var closeTimeGap = ledger ? now - ledger.close_time : Infinity;

    d = (Date.now() - d) / 1000;

    // error from hbase;
    if (err) {
      response.json({
        score: 0,
        responseTime: d + 's',
        message: 'hbase response error',
        error: err
      }).pipe(res);

    // response time over 15 seconds
    } else if (d > 15) {
      response.json({
        score: 1,
        responseTime: d + 's',
        message: 'response time exceeds 15s'
      }).pipe(res);

    // response time over 5 seconds
    } else if (d > 5) {
      response.json({
        score: 2,
        responseTime: d + 's',
        message: 'response time exceeds 5s'
      }).pipe(res);

    // last ledger over 5 minutes ago
    } else if (closeTimeGap > 5 * 60) {
      response.json({
        score: 3,
        responseTime: d + 's',
        closeTimeGap: closeTimeGap + 's',
        message: 'last ledger more than 5 minutes ago'
      }).pipe(res);

    // last ledger over 60 seconds ago
    } else if (closeTimeGap > 60) {
      response.json({
        score: 4,
        responseTime: d + 's',
        closeTimeGap: closeTimeGap + 's',
        message: 'last ledger more than 60 seconds ago'
      }).pipe(res);

    } else {

      d = Date.now();
      hbase.getLastValidated(function(err, resp) {
        d = (Date.now() - d) / 1000;
        var closeTime;
        var validatorGap;

        // error from hbase;
        if (err) {
          response.json({
            score: 0,
            responseTime: d + 's',
            message: 'hbase response error',
            error: err
          }).pipe(res);

        } else {
          closeTime = moment.utc(resp.close_time).unix();
          validatorGap = now - closeTime;

          // validator gap over 5 minutes
          if (validatorGap > 5 * 60) {
            response.json({
              score: 5,
              responseTime: d + 's',
              closeTimeGap: closeTimeGap + 's',
              validatorGap: validatorGap + 's',
              message: 'last validated ledger over 5 minutes ago'
            }).pipe(res);

          // best health
          } else {
            response.json({
              score: 6,
              responseTime: d + 's',
              closeTimeGap: closeTimeGap + 's',
              validatorGap: validatorGap + 's'
            }).pipe(res);
          }
        }
      });
    }
  });
};

module.exports = function(db) {
  hbase = db;
  return checkHealth;
};
