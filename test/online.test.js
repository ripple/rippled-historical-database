var assert  = require('assert');
var Parser  = require('../lib/ledgerParser');
var Rest    = require('../lib/hbase/hbase-rest');
var HBase   = require('../lib/hbase/hbase-client');
var Promise = require('bluebird');
var request  = require('request');
var moment   = require('moment');
var fs      = require('fs');
var Server   = require('../api/server');
var PREFIX  = 'TEST_' + Math.random().toString(36).substr(2, 5) + '_';
var port    = 7111;

var rest = new Rest({
  prefix : PREFIX,
  host   : "54.164.78.183",
  port   : 20550
});

var options = {
  "logLevel" : 2,
  "prefix"   : PREFIX,
  "host"     : "54.172.205.78",
  "port"     : 9090
};

var hbase = new HBase(options);
var path  = __dirname + '/ledgers/';
var files = fs.readdirSync(path);

server = new Server({
  postgres : undefined,
  hbase    : options,
  port     : port,
});


describe('HBASE client and API endpoints', function () {
  before(function(done){
    this.timeout(60000);
    console.log('creating tables in HBASE');
    rest.initTables(function(err, resp) {
      assert.ifError(err);
      done();
    });
  });

  it('should save ledgers into hbase', function(done) {
    this.timeout(60000);
    Promise.map(files, function(filename) {
      return new Promise(function(resolve, reject) {
        var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));
        var parsed = Parser.parseLedger(ledger);

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
      console.log(resp.length, 'ledgers saved');
      done();
    });
  });

  it('should make sure /v1/accounts/:account is not a valid endpoint', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rfU3YWd1TnYryvryQTQ9xwyCSqzMTbnyW6';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      done();
    });
  });

  // This function helps build tests that iterate trough a list of results
  //
  function checkPagination(baseURL, initalMarker, testFunction, done) {
    var url= baseURL + (initalMarker ? '&marker='+initalMarker : '');
    request({
        url: url,
        json: true,
      },
        function (err, res, body) {
          assert.ifError(err);
          assert.strictEqual(res.statusCode, 200);
          assert.strictEqual(typeof body, 'object');
          assert.strictEqual(body.result, 'success');
          return checkIter(body, 0, body.count, initalMarker);
    });
    
    function checkIter(ref, i, imax, marker) {
      if(i < imax) {
        var url= baseURL + (marker ? '&limit=1&marker='+marker : '&limit=1');
        request({
            url: url,
            json: true,
          },
            function (err, res, body) {
              assert.ifError(err);
              assert.strictEqual(res.statusCode, 200);
              assert.strictEqual(typeof body, 'object');
              assert.strictEqual(body.result, 'success');
              testFunction(ref, i, body);
              checkIter(ref, i+1, imax, body.marker);
        });
      } else {
        done();
      }
    }
  }

  // PAYMENTS
  //
  it('should make sure /v1/accounts/:account/payments handles limit correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?limit=2';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 2);
      assert.strictEqual(body.payments.length, 2);      
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/payments handles dates correctly', function(done) {
    var start= '2015-01-14T18:01:00';
    var end= '2015-01-14T18:40:45';
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?'
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        var d= moment.utc(pay.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });      
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/payments handles type correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?'
                                         + 'type=sent';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.source, 'rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx');
      });
      done();
    });
  });

  it('should make sure /v1/accounts/:account/payments handles type correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rGcSxmn1ibh5ZfCMAEu2iy7mnrb5nE6fbY/payments?'
                                         + 'type=received';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.destination, 'rGcSxmn1ibh5ZfCMAEu2iy7mnrb5nE6fbY');
      });
      done();
    });
  });

  it('should make sure /v1/accounts/:account/payments handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?';
    checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.payments.length, 1);
      assert.equal(body.payments[0].amount, ref.payments[i].amount);
      assert.equal(body.payments[0].tx_hash, ref.payments[i].tx_hash);
    }, done);
  });

  it('should make sure /v1/accounts/:account/payments handles empty response correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/payments';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.payments.length, 0);
       assert.strictEqual(body.count, 0);
      done();
    });    
  });

  // EXCHANGES
  //
  it('should make sure /v1/accounts/:account/exhanges handles limit correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?limit=5';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 5);
      assert.strictEqual(body.exchanges.length, 5);      
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/exhanges handles dates correctly', function(done) {
    var start= '2015-01-14T18:52:00';
    var end= '2015-01-14T19:00:00';
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?' 
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        var d= moment.utc(exch.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });          
      done();
    });    
  });  

  it('should make sure /v1/accounts/:account/exhanges/:curr handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/jpy';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'JPY');
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/exhanges/:curr handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/BTC';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'BTC');
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/exhanges/:curr-iss/:counter handles parameters correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'USD');
        assert.strictEqual(exch.base_issuer, 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q');
        assert.strictEqual(exch.counter_currency, 'XRP');        
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/exhanges handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?';
    checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.exchanges.length, 1);
      assert.equal(body.exchanges[0].base_amount, ref.exchanges[i].base_amount);
      assert.equal(body.exchanges[0].base_currency, ref.exchanges[i].base_currency);      
      assert.equal(body.exchanges[0].tx_hash, ref.exchanges[i].tx_hash);
    }, done);
  });

  it('should make sure /v1/accounts/:account/exchanges handles empty response correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/exchanges';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.exchanges.length, 0);
       assert.strictEqual(body.count, 0);
      done();
    });    
  });

  // BALANCE_CHANGES
  //
  it('should make sure /v1/accounts/:account/balance_changes handles limit correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?limit=2';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 2);
      assert.strictEqual(body.balance_changes.length, 2);      
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?currency=xrp';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'XRP');
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?currency=btc';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'BTC');
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles currency correctly', function(done) {
    var issuer= 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?'
                                         + 'currency=btc&issuer='+issuer;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'BTC');
        assert.strictEqual(bch.issuer, issuer);
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?currency=XRP';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'XRP');
      });
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?';
    checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.balance_changes.length, 1);
      assert.equal(body.balance_changes[0].change, ref.balance_changes[i].change);
      assert.equal(body.balance_changes[0].currency, ref.balance_changes[i].currency);      
      assert.equal(body.balance_changes[0].tx_hash, ref.balance_changes[i].tx_hash);
    }, done);
  });

  it('should make sure /v1/accounts/:account/balance_changes handles dates correctly', function(done) {
    var start= '2015-01-14T18:00:00';
    var end= '2015-01-14T18:30:00';
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?' 
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        var d= moment.utc(bch.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });          
      done();
    });    
  }); 

  it('should make sure /v1/accounts/:account/balance_changes handles empty response correctly', function(done) {
    var start= '1015-01-14T18:00:00';
    var end= '1970-01-14T18:30:00';
    var url = 'http://localhost:' + port + '/v1/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?' 
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.balance_changes.length, 0);
      assert.strictEqual(body.count, 0);
      done();
    });    
  }); 

  it('should make sure /v1/accounts/:account/balance_changes handles empty response correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.balance_changes.length, 0);
      assert.strictEqual(body.count, 0);
      done();
    });    
  });

  it('should make sure /v1/accounts/:account/balance_changes handles empty invalid params correctly', function(done) {
    var url = 'http://localhost:' + port + '/v1/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?'
                                         + 'issuer=rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx&currency=Xrp' ;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      done();
    });    
  });

  after(function(done) {
    this.timeout(60000);
    console.log('removing tables');
    rest.removeTables(function(err, resp) {
      done();
    });
  });
});
