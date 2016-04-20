module.exports = function(db) {
  var getMetric = require('./getMetric')(db)
  return {
    exchangeVolume: getMetric.bind(undefined, 'trade_volume'),
    paymentVolume: getMetric.bind(undefined, 'payment_volume'),
    issuedValue: getMetric.bind(undefined, 'issued_value'),
    topMarkets: require('./topMarkets')(db),
    topCurrencies: require('./topCurrencies')(db)
  }
};
