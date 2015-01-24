var assert  = require('assert');
var Stream  = require('../src/lib/ledgerStream');
var Parser  = require('../src/lib/modules/ledgerParser');
var Rest    = require('../src/lib/modules/hbase-rest');
var Promise = require('bluebird');
var fs      = require('fs');

var PREFIX  = 'TEST_';
var rest    = new Rest({
  prefix : PREFIX,
  host   : "54.164.78.183",
  port   : 20550 
});

var stream = new Stream({
  "logLevel" : 3,
  "hbase" : {
    "prefix" : PREFIX,
    "host"   : "54.172.205.78",
    "port"   : 9090
  },
  "ripple" : {
    "trace"                 : false,
    "allow_partial_history" : false,
    "servers" : [
      { "host" : "s-west.ripple.com", "port" : 443, "secure" : true },
      { "host" : "s-east.ripple.com", "port" : 443, "secure" : true }
    ]
  },
});


describe('ledgerStreamSpout', function () {
  before(function(done){
    this.timeout(30000);

    rest.initTables(function(err, resp) {
      assert.ifError(err);
      stream.start();
      
      //wait for a ledger with transactions
      var interval = setInterval(function() {
        if (stream.ledgers.length) {
          if (!stream.ledgers[0].transactions.length) {
            stream.ledgers.shift();
            return;
          }
          
          stream.stop();
          clearInterval(interval);
          done();  
        }  
      }, 100);
    });
  });
  

  it('should process an incoming ledger', function(done) {
    stream.processNextLedger(function(err, resp) {
      done();
    });
  });  
 
  it('should parse metadata of incoming transactions', function(done) {
    stream.parsed = [];
    stream.transactions.forEach(function(tx) {
      stream.parsed.push({
        data        : Parser.parseTransaction(tx),
        ledgerIndex : tx.ledger_index,
        txIndex     : tx.tx_index});
    });
    
    done();
  });    
  
  it('should save incoming transactions', function(done) {
    this.timeout(10000);
    Promise.map(stream.transactions, function(tx) {
      return new Promise (function(resolve, reject) {
        stream.hbase.saveTransaction(tx, function(err, resp) {
          assert.ifError(err);
          if (err) reject(err);
          else     resolve();
        });
      }); 
    }).then(function(){
      done();  
    });
  });   
  
  it('should save parsed transaction data', function(done) {
    this.timeout(10000);
    Promise.map(stream.parsed, function(data) {
      return new Promise (function(resolve, reject) {
        stream.hbase.saveParsedData(data, function(err, resp) {
          assert.ifError(err);
          if (err) reject(err);
          else     resolve();
        });
      }); 
    }).then(function(){
      done();  
    });
  }); 
  
  
  after(function(done) {
    this.timeout(30000);
    console.log('removing tables');
    rest.removeTables(function(err, resp) {
      done();
    });
  });
});
