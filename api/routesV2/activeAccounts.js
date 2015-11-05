'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope : 'get ledger'});
var smoment = require('../../lib/smoment');
var response = require('response');
var periods = ['1day', '3day', '7day'];
var hbase;

function activeAccounts(req, res) {
  var options = {
    base: {},
    counter: {},
    period: req.query.period || '1day',
    includeExchanges : req.query.include_exchanges,
    limit: Infinity,
    format: (req.query.format || 'json').toLowerCase()
  };

  // any of +, |, or .
  var base = req.params.base.split(/[\+|\.]/);
  var counter = req.params.counter.split(/[\+|\.]/);
  var date = smoment(req.query.date);
  options.base.currency = base[0] ? base[0].toUpperCase() : undefined;
  options.base.issuer = base[1] ? base[1] : undefined;

  options.counter.currency = counter[0] ? counter[0].toUpperCase() : undefined;
  options.counter.issuer = counter[1] ? counter[1] : undefined;

  if (!options.base.currency) {
    errorResponse({error: 'base currency is required', code: 400});
  } else if (!options.counter.currency) {
    errorResponse({error: 'counter currency is required', code: 400});
    return;
  } else if (options.base.currency === 'XRP' && options.base.issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.counter.currency === 'XRP' && options.counter.issuer) {
    errorResponse({error: 'XRP cannot have an issuer', code: 400});
    return;
  } else if (options.base.currency !== 'XRP' && !options.base.issuer) {
    errorResponse({error: 'base issuer is required', code: 400});
    return;
  } else if (options.counter.currency !== 'XRP' && !options.counter.issuer) {
    errorResponse({error: 'counter issuer is required', code: 400});
    return;
  } else if (periods.indexOf(options.period) === -1) {
    errorResponse({
      error: 'invalid period - use: ' + periods.join(', '),
      code: 400
    });
    return;
  }

  if (req.query.date && !date) {
    errorResponse({error: 'invalid date format', code: 400});
    return;
  } else if (req.query.date) {
    options.start = date;
    options.end = smoment(date);
    options.end.moment.add(parseInt(options.period, 10), 'day');
  } else {
    options.end = date;
    options.start = smoment(date);
    options.start.moment.subtract(parseInt(options.period, 10), 'day');
  }


  hbase.getExchanges(options, function(err, resp) {

    if (err) {
      console.log(err);
      return;
    }

    successResponse(organizeData(options, resp.rows));
  });

  function organizeData (params, data) {
    var accounts = { };
    var list;

    data.forEach(function(d) {
      if (accounts[d.seller]) {
        accounts[d.seller].sell.base_volume += d.base_amount;
        accounts[d.seller].sell.counter_volume += d.counter_amount;
        accounts[d.seller].sell.count++;
        accounts[d.seller].base_volume += d.base_amount;
        accounts[d.seller].counter_volume += d.counter_amount;
        accounts[d.seller].count++;

      } else {
        accounts[d.seller] = {
          buy: {
            base_volume: 0.0,
            counter_volume: 0.0,
            count: 0
          },
          sell: {
            base_volume: d.base_amount,
            counter_volume: d.counter_amount,
            count: 1
          },
          account: d.seller,
          base_volume: d.base_amount,
          counter_volume: d.counter_amount,
          count: 1
        };
      }

      if (accounts[d.buyer]) {
        accounts[d.buyer].buy.base_volume += d.base_amount;
        accounts[d.buyer].buy.counter_volume += d.counter_amount;
        accounts[d.buyer].buy.count++;
        accounts[d.buyer].base_volume += d.base_amount;
        accounts[d.buyer].counter_volume += d.counter_amount;
        accounts[d.buyer].count++;

      } else {
        accounts[d.buyer] = {
          buy: {
            base_volume: d.base_amount,
            counter_volume: d.counter_amount,
            count: 1
          },
          sell: {
            base_volume: 0.0,
            counter_volume: 0.0,
            count: 0
          },
          account: d.buyer,
          base_volume: d.base_amount,
          counter_volume: d.counter_amount,
          count: 1
        };
      }

      if (params.includeExchanges) {

        if (!accounts[d.seller].exchanges) {
          accounts[d.seller].exchanges = [];
        }

        if (!accounts[d.buyer].exchanges) {
          accounts[d.buyer].exchanges = [];
        }

        d.executed_time = smoment(d.time).moment.toISOString();
        delete d.rowkey;
        delete d.time;

        accounts[d.seller].exchanges.push(d);
        accounts[d.buyer].exchanges.push(d);
      }
    });

    list = Object.keys(accounts).map(function(account) {
      return accounts[account];
    });

    list.sort(function(a, b) {
      return b.base_volume - a.base_volume;
    });

    return {
      accounts: list,
      exchanges_count: data.length
    }
  }

  /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse(err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      response.json({result: 'error', message: err.error})
      .status(err.code).pipe(res);
    } else {
      response.json({result: 'error', message: 'unable to retrieve exchanges'})
      .status(500).pipe(res);
    }
  }

   /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {

    // csv
    if (options.format === 'csv') {
      resp.accounts.forEach(function(d, i) {
        resp.accounts[i] = utils.flattenJSON(d);
      });
      res.csv(resp.accounts, 'active_accounts.csv');

    // json
    } else {
      response.json({
        result: 'success',
        count: resp.accounts.length,
        exchanges_count: resp.exchanges_count,
        accounts: resp.accounts
      }).pipe(res);
    }
  }
}

module.exports = function(db) {
  hbase = db;
  return activeAccounts;
};
