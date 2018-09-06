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
  getManifests: require('./getManifests'),
  getNodes: require('./getNodes'),
  getLinks: require('./getLinks'),
  getTopology: require('./getTopology'),
  getValidatorReports: require('./getValidatorReports'),
  getLedgerValidations: require('./getLedgerValidations'),
  getValidators: require('./getValidators'),
  getValidations: require('./getValidations'),
  getVersions: require('./getVersions')
}

