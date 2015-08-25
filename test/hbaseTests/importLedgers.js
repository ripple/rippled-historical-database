var config = require('../../config/import.config');
var assert = require('assert');
var Parser = require('../../lib/ledgerParser');
var Rest = require('../../lib/hbase/hbase-rest');
var HBase = require('../../lib/hbase/hbase-client');
var Promise = require('bluebird');
var moment = require('moment');
var exAggregation = require('../../lib/aggregation/exchanges');
var statsAggregation = require('../../lib/aggregation/stats');
var paymentsAggregation = require('../../lib/aggregation/accountPayments');

var fs = require('fs');
var path = __dirname + '/../ledgers/';
var files = fs.readdirSync(path);
var hbaseConfig = config.get('hbase');
var statsConfig;
var updates = [];
var exchanges = [];
var payments = [];
var pairs = { };
var hbase;
var stats;
var aggPayments;


hbaseConfig.prefix = config.get('prefix') || 'TEST_';
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 60000;

aggPayments = new paymentsAggregation(hbaseConfig);
stats = new statsAggregation(hbaseConfig);
hbase = new HBase(hbaseConfig);

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
              console.log(ledger.ledger_index, 'saved');
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
          hbase    : hbase,
          logLevel : 3,
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
    this.timeout(10000);
    updates.forEach(function(u) {
      stats.update(u);
    });
    setTimeout(done, 9000);
  });

  it('should aggregate account payments', function(done) {
    this.timeout(10000);
    payments.forEach(function(p) {
      aggPayments.add({
        data: p,
        account: p.source
      });

      aggPayments.add({
        data: p,
        account: p.destination
      });
    });
    setTimeout(done, 9000);
  });
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
