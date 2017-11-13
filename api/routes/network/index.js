'use strict'
var getMetric = require('./getMetric')

module.exports = {
  externalMarkets: require('./externalMarkets'),
  exchangeVolume: getMetric.bind(undefined, 'trade_volume'),
  paymentVolume: getMetric.bind(undefined, 'payment_volume'),
  issuedValue: getMetric.bind(undefined, 'issued_value'),
  xrpDistribution: require('./xrpDistribution'),
  topMarkets: require('./topMarkets'),
  topCurrencies: require('./topCurrencies'),
  getFees: require('./getFees'),
  getFeeStats: require('./getFeeStats'),
  getNodes: require('./getNodes'),
  getLinks: require('./getLinks'),
  getTopology: require('./getTopology'),
  getValidatorReports: require('./getValidatorReports'),
  getValidators: require('./getValidators'),
  getVersions: require('./getVersions')
}

