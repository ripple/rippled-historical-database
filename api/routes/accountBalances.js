var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'account balances'});
var request  = require('request');
var response = require('response');
var postgres;

var accountBalances = function (req, res, next) {

  var options = prepareOptions();

  log.info('ACCOUNT BALANCES:', options.account);

  postgres.getLedger(options, function(err, ledger){
    if (err) {
      errorResponse(err);
    } else {
      getBalances(ledger, options.account);
    }
  });

  /**
  * prepareOptions
  * parse request parameters to determine query options
  */
  function prepareOptions () {
    var options = {
      ledger_index : req.query.ledger_index,
      ledger_hash  : req.query.ledger_hash,
      date         : req.query.date,
      currency     : req.query.currency,
      counterparty : req.query.counterparty,
      limit        : req.query.limit,
      marker       : req.query.marker,
      tx_return    : 'none',
      account      : req.params.address
    };

    return options;
  }

  /**
  * getBalances
  * use ledger_index from getLedger api call
  * to get balances using ripple REST
  */

  function getBalances(ledger, account) {
    var ledger_index = ledger.ledger_index;
    var date         = ledger.close_time_human;
    var balances     = {};
    var url = 'https://api.ripple.com/v1/accounts/'+account+'/balances';
    var body;

    balances.date = date;
    if (!account) errorResponse({error: 'Must provide account.', code:400});

    request({
      url: url,
      json: true,
      qs: {
        currency: options.currency,
        counterparty: options.counterparty,
        limit: options.limit,
        marker: options.marker,
        ledger: ledger_index
      }
    }, function (err, res, body) {

      if (err) {
        errorResponse(err);
        return
      }

      for (var attr in body) {
        balances[attr] = body[attr];
      }

      successResponse(balances);
    });
  }

 /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    if (err.code.toString()[0] === '4') {
      log.error(err.error || err);
      response.json({result:'error', message:err.error}).status(err.code).pipe(res);
    } else {
      response.json({result:'error', message:'unable to retrieve ledger'}).status(500).pipe(res);
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} balances
  */
  function successResponse (balances) {

    if (balances.balances) {
      log.info('ACCOUNT BALANCES: Balances Found:', balances.balances.length);
    } else {
      log.info('ACCOUNT BALANCES: Balances Found: 0');
    }

    response.json(balances).pipe(res);
  }

};

module.exports = function(db) {
  postgres = db;
  return accountBalances;
};
