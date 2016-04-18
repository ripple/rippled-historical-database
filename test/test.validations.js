var config = require('./config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird');
var smoment = require('../lib/smoment');
var moment = require('moment');
var utils = require('./utils');
var Validations = require('../lib/validations/validations');
var mockValidations = require('./mock/validations.json');
var validations;

var hbaseConfig = config.get('hbase');
var port = config.get('port') || 7111;
var prefix = config.get('prefix');

hbaseConfig.prefix = prefix;
validations = new Validations(hbaseConfig);

describe('validations import', function() {
  it('should save validations into hbase', function(done) {
    Promise.map(mockValidations, function(v) {
      return validations.handleValidation(v);
    }).then(function(resp) {
      assert.strictEqual(resp[0], '52E10A015D440A9D35EA0430D78437A2A2416FB3B73E6C56E869FBAF7EE10E47|n9LiNzfbTN5wEc9j2CM9ps7gQqAusVz8amg4gnsfHZ3DWHr2kkG1');
      assert.strictEqual(resp[1], 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5|n9KDJnMxfjH5Ez8DeWzWoE9ath3PnsmkUy3GAHiVjE7tn7Q7KhQ2');
      assert.strictEqual(resp[2], undefined);
      assert.strictEqual(resp[5], undefined);
      done();

    }).catch(function(e) {
      assert.ifError(e);
    });
  });

  it('should save validator reports', function(done) {
    validations.updateReports()
    .then(function(resp) {
      done();

    }).catch(function(e) {
      assert.ifError(e);
    });
  });

  it('should update validator domains', function(done) {
    this.timeout(20000);

    validations.verifyDomains()
    .then(done)
    .catch(e => {
      assert.ifError(e);
    });
  });
});

describe('validator reports', function() {
  it('should get validator reports', function(done) {
    var date = smoment();
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports';

    date.moment.startOf('day');

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.reports.length, 6);
      body.reports.forEach(function(r) {
        assert.strictEqual(r.date, date.format());
      });
      done();
    });
  });


  it('should get validator reports by date', function(done) {
    var date = smoment('2016-01-01');
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?date='+date.format();

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.reports.length, 0);
      done();
    });
  });

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14';
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?date=' + date;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid date format');
      done();
    });
  });

  it ('should get reports by validator', function(done) {
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.reports.length, 1);
      done();
    });
  });

  it('should error on invalid start date', function(done) {
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7';
    var start = 'zzz';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports?start=' + start;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should error on invalid end date', function(done) {
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7';
    var end = 'zzz';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports?end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });

  it('should get validator reports in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?format=csv';

    request({
      url: url
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validator reports.csv');
      done();
    });
  });
});

describe('ledger validations', function() {
  it('should get ledger validations', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.ledger_hash, hash);
      assert.strictEqual(body.validations.length, 4);
      body.validations.forEach(function(d) {
        assert.strictEqual(d.ledger_hash, hash);
      });
      done();
    });
  });

  it('should handle /ledgers/:hash/validations pagination correctly', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.validations.length, 1);
      assert.equal(body.validations[0].signature, ref.validations[i].signature);
    }, done);
  });

  it('should include a link header when marker is present', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?limit=1';
    var linkHeader = '<' + url +
      '&marker=EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5|n9KDJnMxfjH5Ez8DeWzWoE9ath3PnsmkUy3GAHiVjE7tn7Q7KhQ2>; rel="next"';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers.link, linkHeader);
      done();
    });
  });


  it('should get ledger validations in CSV format', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?format=csv';

    request({
      url: url
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=' + hash + ' - validations.csv');
      done();
    });
  });

  it('should get a specific ledger validation', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations/' + pubkey;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.ledger_hash, hash);
      assert.strictEqual(body.validation_public_key, pubkey);
      done();
    });
  });

  it('should error on an invalid ledger hash', function(done) {
    var hash = 'zzz';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?format=csv';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid ledger hash');
      done();
    });
  });

  it('should error on validation not found', function(done) {
    var hash = 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5';
    var pubkey = 'abcd';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations/' + pubkey;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'validation not found');
      done();
    });
  });
});


describe('validators', function() {
  it('should get all validators', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validators';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validators.length, 6);
      done();
    });
  });

  it('should get a single validator', function(done) {
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validation_public_key, pubkey);
      done();
    });
  });

  it('should get error on validator not found', function(done) {
    var pubkey = 'zzz';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      done();
    });
  });

  it('should get validators in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validators?format=csv';

    request({
      url: url
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validators.csv');
      done();
    });
  });
});

describe('validations', function() {
  it('should get validations', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validations';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validations.length, 6);
      done();
    });
  });

  it('should limit results based on start date', function(done) {
    var start = moment.utc().add(1, 'day').format('YYYY-MM-DD');
    var url = 'http://localhost:' + port +
        '/v2/network/validations?start=' + start;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validations.length, 0);
      done();
    });
  });

  it('should limit results based on end date', function(done) {
    var end = moment.utc().subtract(1, 'day').format('YYYY-MM-DD');
    var url = 'http://localhost:' + port +
        '/v2/network/validations?end=' + end;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validations.length, 0);
      done();
    });
  });

  it('should get validations by validator public key', function(done) {
    var pubkey = 'n9KDJnMxfjH5Ez8DeWzWoE9ath3PnsmkUy3GAHiVjE7tn7Q7KhQ2';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/validations';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.validations.length, 1);
      done();
    });
  });

  it('should handle /network/validations pagination correctly', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validations?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.validations.length, 1);
      assert.equal(body.validations[0].signature, ref.validations[i].signature);
    }, done);
  });


  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validations?limit=2';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof res.headers.link, 'string');
      done();
    });
  });

  it('should get validations in CSV format', function(done) {
 var url = 'http://localhost:' + port +
        '/v2/network/validations?format=csv';

    request({
      url: url
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validations.csv');
      done();
    });
  });

  it('should error on invalid start', function(done) {
    var date = 'zzz2015-01-14';
    var url = 'http://localhost:' + port +
        '/v2/network/validations?start=' + date;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should error on invalid end', function(done) {
    var date = 'zzz2015-01-14';
    var url = 'http://localhost:' + port +
        '/v2/network/validations?end=' + date;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });
});
