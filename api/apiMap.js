'use strict';

var packageJSON = require('../package.json');

function generateMap(req, res) {
  var url = 'https://data.ripple.com/v2';
  var repo = 'https://github.com/ripple/rippled-historical-database';
  var json = {
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
        action: 'Get Exchanges',
        route: '/v2/exchanges/{:base}/{:counter}',
        example: url + '/exchanges/XRP/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'
      }, {
        action: 'Get Exchange Rate',
        route: '/v2/exchange_rates/{:base}/{:counter}',
        example: url + '/exchange_rates/XRP/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'
      }, {
        action: 'Normalize Amount',
        route: '/v2/normalize',
        example: url + '/normalize?amount=2000&currency=XRP&exchange_currency=USD' +
        '&exchange_issuer=rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'
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

  res.setHeader('Content-Type', 'application/json');
  res.send(JSON.stringify(json, undefined, 2));
}

module.exports = generateMap;
