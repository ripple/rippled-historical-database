var smoment = require('../../lib/smoment');

module.exports = function(ledger) {
  var data = {
    total: 0,
    ledger_index: ledger.ledger_index,
    date: smoment(ledger.close_time).format(),
    tx_count: ledger.transactions.length,
    avg: 0,
    max: 0,
    min: Infinity
  };

  if (ledger.transactions.length) {
    ledger.transactions.forEach(function(tx) {
      var fee = Number(tx.Fee);
      data.total += fee;

      data.max = fee > data.max ? fee : data.max;
      data.min = fee < data.min ? fee : data.min;
    });

    data.max /= 1000000;
    data.min /= 1000000;
    data.total /= 1000000;
    data.avg = data.total / data.tx_count;
    data.avg = Number(data.avg.toPrecision(6));
  } else {
    data.min = 0;
  }

  return data;
};
