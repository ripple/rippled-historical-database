module.exports = function(db) {
  var getMetric = require('./getMetric')(db)
  return {
    exchangeVolume: getMetric.bind(undefined, 'trade_volume'),
    paymentVolume: getMetric.bind(undefined, 'payment_volume'),
    issuedValue: getMetric.bind(undefined, 'issued_value'),
    xrpDistribution: require('./xrpDistribution')(db),
    topMarkets: require('./topMarkets')(db),
    topCurrencies: require('./topCurrencies')(db),
    getFees: require('./getFees')(db),
    getFeeStats: require('./getFeeStats')(db),
    getNodes: require('./getNodes')(db),
    getLinks: require('./getLinks')(db),
    getTopology: require('./getTopology')(db),
    getValidatorReports: require('./getValidatorReports')(db),
    getLedgerValidations: require('./getLedgerValidations')(db),
    getValidators: require('./getValidators')(db),
    getValidations: require('./getValidations')(db),
    getVersions: require('./getVersions')(db)
  }
};
