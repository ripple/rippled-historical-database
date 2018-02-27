/* eslint no-unused-vars: ["error", { "args": "after-used" }] */
'use strict'

var express = require('express')
var bodyParser = require('body-parser')
var compression = require('compression')
var cors = require('cors')
var routes = require('./routes')
var map = require('./apiMap')
var json2csv = require('nice-json2csv')
var favicon = require('serve-favicon')
var rateLimitMiddleware = require('../lib/rateLimit').middleware

/**
 * filterDuplicateQueryParams
 * NOTE: this only works if we dont pass
 * an array as a query param intentionally
 */

function filterDuplicateQueryParams(req, res, next) {

  for (var key in req.query) {
    if (Array.isArray(req.query[key])) {
      req.query[key] = req.query[key][0]
    }
  }

  next()
}

/**
 * Server
 */

function Server(options) {

  var app = express()
  var cacheControl = ''
  var server

  /**
  * setCacheControl
  */

  function setCacheControl(req, res, next) {
    res.setHeader('Cache-Control', cacheControl)
    next()
  }

  Object.keys(options.cacheControl || {}).forEach(function(key) {
    cacheControl += key + '=' + options.cacheControl[key] + ', '
  })

  console.log('CACHE: ' + (cacheControl || 'none set'))

  app.use(bodyParser.json())
  app.use(bodyParser.urlencoded({extended: true}))
  app.use(json2csv.expressDecorator)
  app.use(cors())
  app.use(filterDuplicateQueryParams)
  app.use(favicon(__dirname + '/favicon.png'))
  app.use(compression())
  app.use(setCacheControl)
  app.use(rateLimitMiddleware)

  app.get('/v2/health/:aspect?', routes.checkHealth)
  app.get('/v2/gateways/:gateway?', routes.gateways.Gateways)
  app.get('/v2/gateways/:gateway/assets/:filename?', routes.gateways.Assets)
  app.get('/v2/currencies/:currencyAsset?', routes.gateways.Currencies)
  app.get('/v2/active_accounts/:base/:counter', routes.activeAccounts)
  app.get('/v2/network/exchange_volume', routes.network.exchangeVolume)
  app.get('/v2/network/payment_volume', routes.network.paymentVolume)
  app.get('/v2/network/external_markets', routes.network.externalMarkets)
  app.get('/v2/network/xrp_distribution', routes.network.xrpDistribution)
  app.get('/v2/network/top_markets/:date?', routes.network.topMarkets)
  app.get('/v2/network/top_currencies/:date?', routes.network.topCurrencies)
  app.get('/v2/network/fees', routes.network.getFees)
  app.get('/v2/network/fee_stats', routes.network.getFeeStats)
  app.get('/v2/network/topology', routes.network.getTopology)
  app.get('/v2/network/topology/nodes', routes.network.getNodes)
  app.get('/v2/network/topology/nodes/:pubkey', routes.network.getNodes)
  app.get('/v2/network/topology/links', routes.network.getLinks)
  app.get('/v2/network/validators', routes.network.getValidators)
  app.get('/v2/network/validators/:pubkey', routes.network.getValidators)
  app.get('/v2/network/validators/:pubkey/manifests',
          routes.network.getManifests)
  app.get('/v2/network/validators/:pubkey/reports',
          routes.network.getValidatorReports)
  app.get('/v2/network/validator_reports', routes.network.getValidatorReports)
  app.get('/v2/network/rippled_versions', routes.network.getVersions)
  app.get('/v2/last_validated', routes.getLastValidated)
  app.get('/v2/transactions/', routes.getTransactions)
  app.get('/v2/transactions/:tx_hash', routes.getTransactions)
  app.get('/v2/ledgers/:ledger_param?', routes.getLedger)
  app.get('/v2/ledgers/:ledger_hash/validations',
          routes.network.getLedgerValidations)
  app.get('/v2/ledgers/:ledger_hash/validations/:validation_pubkey',
          routes.network.getLedgerValidations)
  app.get('/v2/accounts', routes.accounts)
  app.get('/v2/accounts/:address', routes.getAccount)
  app.get('/v2/accounts/:address/transactions/:sequence', routes.accountTxSeq)
  app.get('/v2/accounts/:address/transactions', routes.accountTransactions)
  app.get('/v2/accounts/:address/balances', routes.accountBalances)
  app.get('/v2/accounts/:address/payments', routes.accountPayments)
  app.get('/v2/accounts/:address/reports/:date?', routes.accountReports)
  app.get('/v2/accounts/:address/escrows', routes.accountEscrows)
  app.get('/v2/accounts/:address/payment_channels',
          routes.accountPaymentChannels)
  app.get('/v2/accounts/:address/balance_changes', routes.getChanges)
  app.get('/v2/accounts/:address/exchanges', routes.accountExchanges)
  app.get('/v2/accounts/:address/exchanges/:base', routes.accountExchanges)
  app.get('/v2/accounts/:address/exchanges/:base/:counter',
          routes.accountExchanges)
  app.get('/v2/accounts/:address/orders', routes.accountOrders)
  app.get('/v2/accounts/:address/stats/:family', routes.accountStats)
  app.get('/v2/accounts', routes.accounts)
  app.get('/v2/payments/:currency?', routes.getPayments)
  app.get('/v2/exchanges/:base/:counter', routes.getExchanges)
  app.get('/v2/exchange_rates/:base/:counter', routes.getExchangeRate)
  app.get('/v2/normalize', routes.normalize)
  app.get('/v2/reports/:date?', routes.reports)
  app.get('/v2/stats', routes.stats)
  app.get('/v2/stats/:family', routes.stats)
  app.get('/v2/stats/:family/:metric', routes.stats)
  app.get('/v2/maintenance/:domain', routes.maintenance)
  app.get('/v2/estimate', routes.estimate)
  app.get('/v2/xrp_index', routes.xrpIndex)
  app.get('/v2/xrp_index/ticker', routes.xrpIndexTicker)

  // index page
  app.get('/', map.generate)
  app.get('/v2', map.generate)

  // 404
  app.get('*', map.generate404)
  app.post('*', map.generate404)


  // start the server
  server = app.listen(options.port)
  console.log('Ripple Data API running on port: ' + options.port)

  // log error
  server.on('error', function(err) {
    console.log(err)
  })

  // log close
  server.on('close', function() {
    console.log('server on port: ' + options.port + ' closed')
  })

  this.close = function() {
    if (server) {
      server.close()
      console.log('closing API on port: ' + options.port)
    }
  }
}

module.exports = Server
