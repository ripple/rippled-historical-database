var request = require('request');

var API = 'https://data.ripple.com/v2/ledgers';
var RPC = 'https://s2.ripple.com:51234';
var account;
var hotwallets;
var date = '';
var ledger;

console.log('');

process.argv.forEach(function (d) {
  var value = d.split(':');
  var key = value.shift();
  value = value.join(':');

  if (key === 'account') {
    account = value;

  } else if (key === 'hotwallets') {
    hotwallets = value.split(',');

  } else if (key = 'date') {
    date = '/' + value;

  } else if (key === 'ledger') {
    ledger = value;
  }
});

if (!account) {
  console.log('account is required');
  return;
}

if (ledger) {
  getBalances();

} else {
  request.get({
    url: API + date,
    json: true
  }, function(err, resp, body) {
    if (err) {
      console.log('data API error:')
      console.log(err);
      return;
    }

    if (body.ledger) {
      console.log('ledger:', body.ledger.ledger_index);
      console.log('close time:', body.ledger.close_time_human);
      ledger = body.ledger.ledger_index;
      getBalances();

    } else {
      console.log(body);
    }

  });
}

function getBalances() {
  request.post({
    url: RPC,
    json: true,
    body: {
      method: 'gateway_balances',
      params: [{
        account: account,
        ledger_index: ledger || "validated",
        hotwallet: hotwallets || [],
        strict: true
      }]
    }
  }, function (err, resp, body) {
    if (err) {
      console.log('rippled error:')
      console.log(err);
      return;
    }

    if (body.result) {
      console.log('ledger hash:', body.result.ledger_hash);
      console.log('');

      if (ledger !== body.result.ledger_index) {
        console.log('WARINING - different ledger index:', body.result.ledger_index);
        console.log('');
      }

      if (body.result.assets) {
        console.log('assets:')
        console.log(pretty(body.result.assets));
      }

      if (body.result.balances) {
        console.log('hot wallets:')
        console.log(pretty(body.result.balances));
      }

      if (body.result.obligations) {
        console.log('obligations:')
        console.log(pretty(body.result.obligations));
      }
    } else {
      console.log(body);
    }
  });
}

function pretty(json) {
  return JSON.stringify(json, undefined, 2)+'\n';
}

