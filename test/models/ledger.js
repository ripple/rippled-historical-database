var assert = require('assert');
var Ledger = require(__dirname+'/../../lib/models/ledger.js');


describe('Ledger sequelize model', function() {

  it('should instantiate a new ledger', function() {
    var ledger = Ledger.build();
    assert(ledger);
  });

  it('should retrive all ledger ids', function(done) {
    Ledger.setOfSavedLedgers(function(error, set) {
      //console.log(error, set);
      done();
    });
  });

});

