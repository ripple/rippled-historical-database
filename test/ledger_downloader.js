var assert = require('assert');
var LedgerDownloader = require('../lib/ledger_downloader');

describe('Ledger Downloader', function() {

  var ledger_downloader = new LedgerDownloader();

  it('should get latest ledger index number', function(done) {
    ledger_downloader.latestIndexNumber(function(err, index) {
      if (err) return err;
      assert(typeof index === 'number');
      assert(index > 0);
      done();
    });
  });

  it('should get a particular ledger index', function(done) {
    ledger_downloader.getIndex(7000001, function(err, ledger) {
      if (err) return err;
      assert(typeof ledger === 'object');
      done();
    });
  });

});
