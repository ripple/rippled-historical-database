var config = require('../config')
config.file('defaults', __dirname + '/test_config.json')

var assert = require('assert')
var Parser = require('../lib/ledgerParser')
var Rest = require('../lib/hbase/hbase-rest')
var Promise = require('bluebird')
var moment = require('moment')
var exAggregation = require('../lib/aggregation/exchanges')
var statsAggregation = require('../lib/aggregation/stats')
var paymentsAggregation = require('../lib/aggregation/payments')
var accountPaymentsAggregation = require('../lib/aggregation/accountPayments')
var feesAggregation = require('../lib/aggregation/fees')

var hbase = require('../lib/hbase')
var fs = require('fs');
var path = __dirname + '/mock/ledgers/';
var files = fs.readdirSync(path);
var updates = [];
var exchanges = [];
var payments = [];
var fees = [];
var pairs = { };
var hbase;
var stats;
var aggPayments;
var aggFees;

aggAccountPayments = new accountPaymentsAggregation()
aggFees = new feesAggregation()
stats = new statsAggregation()

describe('import ledgers', function(done) {
  it('should save ledgers into hbase', function(done) {
    this.timeout(60000);
    Promise.map(files, function(filename) {
      return new Promise(function(resolve, reject) {
        var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));
        var parsed = Parser.parseLedger(ledger);

        //save exchanges
        exchanges.push.apply(exchanges, parsed.exchanges);

        //save payments
        payments.push.apply(payments, parsed.payments);

        //save fees
        fees.push(parsed.feeSummary);

        //save stats
        addStats(parsed);
        updates.push({
          label : 'ledger_count',
          data  : {
            time         : ledger.close_time,
            ledger_index : ledger.ledger_index,
            tx_count     : ledger.transactions.length
          }
        });

        hbase.saveParsedData({data:parsed}, function(err, resp) {
          assert.ifError(err);
          hbase.saveTransactions(parsed.transactions, function(err, resp) {
            assert.ifError(err);
            hbase.saveLedger(parsed.ledger, function(err, resp) {
              assert.ifError(err);
              console.log('saved ledger:', ledger.ledger_index);
              resolve();
            });
          });
        });
      });
    }).nodeify(function(err, resp) {
      assert.ifError(err);
      console.log(resp.length + ' ledgers saved');
      done(err);
    });
  });

  it('should aggregate network fees', function(done) {
    this.timeout(7000);
    Promise.map(fees, function(feeSummary) {
      return aggFees.handleFeeSummary(feeSummary);
    })
    .then(function() {
      done();
    })
    .catch(function(e) {
      assert.ifError(e);
    });
  });

  it('should save exchanges into hbase', function(done) {
    this.timeout(15000);
    exchanges.forEach(function(ex, i) {
      var pair = ex.base.currency +
        (ex.base.issuer ? "." + ex.base.issuer : '') +
        '/' + ex.counter.currency +
        (ex.counter.issuer ? "." + ex.counter.issuer : '');

      if (!pairs[pair]) {
        pairs[pair] = new exAggregation({
          base     : ex.base,
          counter  : ex.counter,
          earliest : moment.unix(ex.time).utc()
        });
      }

      //console.log(pair);
      pairs[pair].add(ex, function(err, resp) {
        if (err) console.log(err);
        if (i===exchanges.length-1) {
          done();
        }
      });
    });
  });

  // NOTE: would be better to have a callback here
  it('should save stats into hbase', function(done) {
    this.timeout(5000);
    updates.forEach(function(u) {
      stats.update(u);
    });
    setTimeout(done, 4500);
  });

  it('should aggregate account payments', function(done) {
    this.timeout(7000);
    payments.forEach(function(p) {
      aggAccountPayments.add({
        data: p,
        account: p.source
      });

      aggAccountPayments.add({
        data: p,
        account: p.destination
      });
    });
    setTimeout(done, 6500);
  });

  it('should aggregate payments', function(done) {
    this.timeout(4000);
    var currencies = {}
    var counter = 0

    payments.forEach(function(p) {
      var key = p.currency + p.issuer

      if (!currencies[key]) {
        currencies[key] = new paymentsAggregation({
          currency: p.currency,
          issuer: p.issuer,
        });
      }

      currencies[key].add(p, function(err) {
        if (++counter === payments.length) {
          done()
        }
      })
    })
  })
});

function addStats (parsed) {

  //save transaction stats
  parsed.transactions.forEach(function(tx) {
      //increment transactions
    updates.push({
      label : 'transaction_count',
      data  : {
        count  : 1,
        time   : tx.executed_time
      }
    });

    //aggregate by transaction type
    updates.push({
      label : 'transaction_type',
      data  : {
        type   : tx.TransactionType,
        time   : tx.executed_time
      }
    });

    //aggregate by transaction result
    updates.push({
      label : 'transaction_result',
      data  : {
        result : tx.tx_result,
        time   : tx.executed_time
      }
    });
  });

  //new account created
  parsed.accountsCreated.forEach(function(a) {
    updates.push({
      label : 'accounts_created',
      data  : {
        count  : 1,
        time   : a.time
      }
    });
  });

  //aggregate payments count
  parsed.payments.forEach(function(p) {
    updates.push({
      label : 'payments_count',
      data  : {
        count  : 1,
        time   : p.time
      }
    });
  });


  //aggregate exchanges count
  parsed.exchanges.forEach(function(ex) {
    updates.push({
      label : 'exchanges_count',
      data  : {
        count  : 1,
        time   : ex.time
      }
    });
  });
}
