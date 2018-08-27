'use strict'

var packageJSON = require('../package.json')
function generateMap(url) {
  var repo = 'https://github.com/ripple/rippled-historical-database'
  return {
    'name': packageJSON.name,
    'version': packageJSON.version,
    'documentation': repo,
    'release-notes': repo + '/releases/tag/v' + packageJSON.version,
    'endpoints': [
      {
        action: 'Get Account Transactions',
        route: '/v2/accounts/{:address}/transactions',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/transactions'
      }, {
        action: 'Get Account Transactions By Sequence',
        route: '/v2/accounts/{:address}/transactions/{:sequence}',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/transactions/112'
      }, {
        action: 'Get Account Payments',
        route: '/v2/accounts/{:address}/payments',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/payments'
      }, {
        action: 'Get Account Exchanges',
        route: '/v2/accounts/{:address}/exchanges',
        example: url + '/accounts/' +
          'rQaxmmRMasgt1edq8pfJKCfbkEiSp5FqXJ/exchanges'
      }, {
        action: 'Get Account Balance Changes',
        route: '/v2/accounts/{:address}/balance_changes',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/balance_changes'
      }, {
        action: 'Get Account Reports',
        route: '/v2/accounts/{:address}/reports/{:date}',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/reports/2013-02-01'
      }, {
        action: 'Get Account Balances',
        route: '/v2/accounts/{:address}/balances',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/balances'
      }, {
        action: 'Get Account Orders',
        route: '/v2/accounts/{:address}/orders',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/orders'
      }, {
        action: 'Get Account Transaction Stats',
        route: '/v2/accounts/{:address}/stats/transactions',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/stats/transactions'
      }, {
        action: 'Get Account Value Stats',
        route: '/v2/accounts/{:address}/stats/value',
        example: url + '/accounts/' +
          'r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/stats/value'
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
        example: url + '/transactions/' +
        '3B1A4E1C9BB6A7208EB146BCDB86ECEA6068ED01466D933528CA2B4C64F753EF'
      }, {
        action: 'Get Payments',
        route: '/v2/payments/{:currency+issuer}',
        example: url + '/payments/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Exchanges',
        route: '/v2/exchanges/{:base}/{:counter}',
        example: url + '/exchanges/XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Active Accounts',
        route: '/v2/active_accounts/{:base}/{:counter}',
        example: url + '/active_accounts/XRP/' +
          'USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Exchange Volume',
        route: '/v2/network/exchange_volume',
        example: url + '/network/exchange_volume'
      }, {
        action: 'Get Payment Volume',
        route: '/v2/network/payment_volume',
        example: url + '/network/payment_volume'
      }, {
        action: 'Get External Market Volume',
        route: '/v2/network/external_markets',
        example: url + '/network/external_markets?period=3day'
      }, {
        action: 'Get XRP Distribution',
        route: '/v2/network/xrp_distribution',
        example: url + '/network/xrp_distribution'
      }, {
        action: 'Get Top Currencies',
        route: '/v2/network/top_currencies/{:date}',
        example: url + '/network/top_currencies'
      }, {
        action: 'Get Top Markets',
        route: '/v2/network/top_markets/{:date}',
        example: url + '/network/top_markets'
      }, {
        action: 'Get Network Topology',
        route: '/v2/network/topology',
        example: url + '/network/topology'
      }, {
        action: 'Get Network Topology Nodes',
        route: '/v2/network/topology/nodes',
        example: url + '/network/topology/nodes'
      }, {
        action: 'Get Network Topology Node by public key',
        route: '/v2/network/topology/nodes/:',
        example: url + '/network/topology/nodes/' +
        'n94JjtkVyx6oTN1Rxs6RyxB9xCQB7NHpv5ibStmNHVQtDAZMJ2LB'
      }, {
        action: 'Get Network Topology Links',
        route: '/v2/network/topology/links',
        example: url + '/network/topology/links'
      }, {
        action: 'Get Validators',
        route: '/v2/network/validators',
        example: url + '/network/validators'
      }, {
        action: 'Get Validator',
        route: '/v2/network/validators/{:pubkey}',
        example: url + '/network/validators/' +
        'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7'
      }, {
        action: 'Get Validator Validations',
        route: '/v2/network/validators/{:pubkey}/validations',
        example: url + '/network/validators/' +
        'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7/validations'
      }, {
        action: 'Get Validator Reports',
        route: '/v2/network/validators/{:pubkey}/reports',
        example: url + '/network/validators/' +
          'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7/reports'
      }, {
        action: 'Get Validator Reports',
        route: '/v2/network/validator_reports',
        example: url + '/network/validator_reports'
      }, {
        action: 'Get Validations',
        route: '/v2/network/validations',
        example: url + '/network/validations'
      }, {
        action: 'Get Rippled Versions',
        route: '/v2/network/rippled_versions',
        example: url + '/network/rippled_versions'
      }, {
        action: 'Get Exchange Rate',
        route: '/v2/exchange_rates/{:base}/{:counter}',
        example: url + '/exchange_rates/XRP/' +
          'USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Normalize Amount',
        route: '/v2/normalize',
        example: url + '/normalize?amount=2000&currency=XRP' +
          '&exchange_currency=USD' +
          '&exchange_issuer=rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }, {
        action: 'Get Daily Summary',
        route: '/v2/reports/{:date}',
        example: url + '/reports'
      }, {
        action: 'Get Transaction Statistics',
        route: '/v2/stats/{:family}/{:metric}',
        example: url + '/stats'
      }, {
        action: 'Check Health',
        route: '/v2/health/{:component}',
        example: url + '/health/importer?verbose=true'
      }
    ]
  }
}

function generate(req, res) {
  var url = req.protocol + '://' + req.get('host') + '/v2'
  var map = generateMap(url)
  res.setHeader('Content-Type', 'application/json')
  res.send(JSON.stringify(map, undefined, 2))
}

function generate404(req, res) {
  var url = req.protocol + '://' + req.get('host') + '/v2'
  var data = {
    result: 'error',
    message: 'Cannot ' + req.method + ' ' + req.originalUrl,
    'api-map': generateMap(url)
  }

  res.setHeader('Content-Type', 'application/json')
  res.status(404).send(JSON.stringify(data, undefined, 2))
}

function generateDeprecated(req, res) {
  var data = {
    result: 'error',
    message: 'This endpoint has been deprecated: ' + req.originalUrl
  }

  res.setHeader('Content-Type', 'application/json')
  res.status(410).send(JSON.stringify(data, undefined, 2))
}

module.exports = {
  generate: generate,
  generate404: generate404,
  deprecated: generateDeprecated
}
