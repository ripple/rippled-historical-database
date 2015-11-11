'use strict';

var packageJSON = require('../package.json');
var generateMap = function(url) {
  var repo = 'https://github.com/ripple/rippled-historical-database';
  return {
    'name': packageJSON.name,
    'version': packageJSON.version,
    'documentation': repo,
    'release-notes': repo + '/releases/tag/v' + packageJSON.version,
    'endpoints': [
      {
        action: 'Get Account Transactions',
        route: '/v2/accounts/{:address}/transactions',
        example: url + '/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/transactions'
      }, {
        action: 'Get Account Transactions By Sequence',
        route: '/v2/accounts/{:address}/transactions/{:sequence}',
        example: url + '/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/transactions/112'
      }, {
        action: 'Get Account Payments',
        route: '/v2/accounts/{:address}/payments',
        example: url + '/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/payments'
      }, {
        action: 'Get Account Exchanges',
        route: '/v2/accounts/{:address}/exchanges',
        example: url + '/accounts/rQaxmmRMasgt1edq8pfJKCfbkEiSp5FqXJ/exchanges'
      }, {
        action: 'Get Account Balance Changes',
        route: '/v2/accounts/{:address}/balance_changes',
        example: url + '/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/balance_changes'
      }, {
        action: 'Get Account Reports',
        route: '/v2/accounts/{:address}/reports/{:date}',
        example: url + '/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/reports/2013-02-01'
      }, {
        action: 'Get Account',
        route: '/v2/accounts/{:address}',
        example: url + '/accounts/rB31eWvkfKBAu6FDD9zgnzT4RwSfXGcqPm'
      }, {
        action: 'Get Accounts',
        route: '/v2/accounts',
        example: url + '/accounts?parent=r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV'
      }, {
        action: 'Get Ledgers',
        route: '/v2/ledgers/{:ledger_hash/ledger_index/date}',
        example: url + '/ledgers'
      }, {
        action: 'Get Transactions',
        route: '/v2/transactions',
        example: url + '/transactions?start=2015-08-01'
      }, {
        action: 'Get Transaction',
        route: '/v2/transactions/{:tx_hash}',
        example: url + '/transactions/3B1A4E1C9BB6A7208EB146BCDB86ECEA6068ED01466D933528CA2B4C64F753EF'
      }, {
        action: 'Get Payments',
        route: '/v2/payments/{:currency+issuer}',
        example: url + '/payments/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Exchanges',
        route: '/v2/exchanges/{:base}/{:counter}',
        example: url + '/exchanges/XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Capitalization',
        route: '/v2/capitalization/{:currency+issuer}',
        example: url + '/capitalization/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Active Accounts',
        route: '/v2/active_accounts/{:base}/{:counter}',
        example: url + '/active_accounts/XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Exchange Volume',
        route: '/v2/network/exchange_volume',
        example: url + '/network/exchange_volume'
      }, {
        action: 'Get Payment Volume',
        route: '/v2/network/payment_volume',
        example: url + '/network/payment_volume'
      }, {
        action: 'Get Issued Value',
        route: '/v2/network/issued_value',
        example: url + '/network/issued_value'
      }, {
        action: 'Get Exchange Rate',
        route: '/v2/exchange_rates/{:base}/{:counter}',
        example: url + '/exchange_rates/XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Normalize Amount',
        route: '/v2/normalize',
        example: url + '/normalize?amount=2000&currency=XRP&exchange_currency=USD' +
        '&exchange_issuer=rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Daily Summary',
        route: '/v2/reports/{:date}',
        example: url + '/reports'
      }, {
        action: 'Get Transaction Statistics',
        route: '/v2/stats/{:family}/{:metric}',
        example: url + '/stats'
      }
    ]
  };
}

var generate = function(req, res) {
  var url = req.protocol + '://' + req.get('host') + '/v2';
  var map = generateMap(url);
  res.setHeader('Content-Type', 'application/json');
  res.send(JSON.stringify(map, undefined, 2));
}

var generate404 = function(req, res) {
  var url = req.protocol + '://' + req.get('host') + '/v2';
  var data = {
    result: 'error',
    message: 'Cannot ' + req.method + ' ' + req.originalUrl,
    'api-map': generateMap(url)
  };

  res.setHeader('Content-Type', 'application/json');
  res.status(404).send(JSON.stringify(data, undefined, 2));
}

module.exports = {
  generate: generate,
  generate404: generate404
};
