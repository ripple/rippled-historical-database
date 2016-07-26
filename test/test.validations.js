var config = require('./config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird');
const Hbase = require('../lib/hbase/hbase-client');
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

const hbase = new Hbase(hbaseConfig);

describe('handleValidation', function(done) {
  var tmp_validations

  beforeEach(function(done) {
    tmp_validations = new Validations(hbaseConfig);
    hbase.deleteAllRows({
      table: 'validations_by_ledger'
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validations_by_validator'
      })
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validations_by_date'
      })
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validators'
      })
    }).then(() => { done(); })
  });

  after(function(done) {
    hbase.deleteAllRows({
      table: 'validations_by_ledger'
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validations_by_validator'
      })
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validations_by_date'
      })
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'validators'
      })
    }).then(() => { done(); })
  });

  it('should save validations into hbase', function(done) {
    const validation = {
      amendments:[
        "42426C4D4F1009EE67080A9B7965B44656D7714D104A72F9B4369F97ABF044EE",
        "4C97EBA926031A7CF7D7B36FDE3ED66DDA5421192D63DE53FFB46E43B9DC8373",
        "6781F8368C4771B83E8B821D88F580202BCB4228075297B19E4FDC5233F1EFDC",
        "C1B8D934087225F509BEB5A8EC24447854713EE447D277F69545ABFA0E0FD490",
        "DA1BD556B42D85EA9C84066D028D355B52416734D3283F85E216EA5DA6DB7E13"
      ],
      base_fee:4503599627370495,
      flags:2147483648,
      full:true,
      ledger_hash:"EC02890710AAA2B71221B0D560CFB22D64317C07B7406B02959AD84BAD33E602",
      ledger_index:6,
      load_fee:256000,
      reserve_base:20000000,
      reserve_inc:5000000,
      signature:"3045022100E199B55643F66BC6B37DBC5E185321CF952FD35D13D9E8001EB2564FFB94A07602201746C9A4F7A93647131A2DEB03B76F05E426EC67A5A27D77F4FF2603B9A528E6",
      signing_time:515115322,
      validation_public_key:"n94Gnc6svmaPPRHUAyyib1gQUov8sYbjLoEwUBYPH39qHZXuo8ZT"
    }
    tmp_validations.handleValidation(validation)
    .then(() => {
      return hbase.getAllRows({
        table: 'validations_by_ledger'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      assert.strictEqual(rows[0].ledger_hash, validation.ledger_hash);
      var row_amendments = JSON.parse(rows[0].amendments)
      assert.strictEqual(row_amendments.length, validation.amendments.length);
      for (var i=0; i<validation.amendments.length; i++) {
        assert.strictEqual(row_amendments[i], validation.amendments[i]);
      }
      assert.strictEqual(rows[0].base_fee, validation.base_fee.toString());
      assert.strictEqual(rows[0].flags, validation.flags.toString());
      assert.strictEqual(rows[0].full, validation.full.toString());
      assert.strictEqual(rows[0].ledger_index, validation.ledger_index.toString());
      assert.strictEqual(rows[0].load_fee, validation.load_fee.toString());
      assert.strictEqual(rows[0].reserve_base, validation.reserve_base.toString());
      assert.strictEqual(rows[0].reserve_inc, validation.reserve_inc.toString());
      assert.strictEqual(rows[0].signature, validation.signature);
      assert.strictEqual(rows[0].signing_time, validation.signing_time.toString());
      return hbase.getAllRows({
        table: 'validations_by_validator'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      assert.strictEqual(rows[0].ledger_hash, validation.ledger_hash);
      return hbase.getAllRows({
        table: 'validations_by_date'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      assert.strictEqual(rows[0].ledger_hash, validation.ledger_hash);
      return hbase.getAllRows({
        table: 'validators'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      done();
    });
  });

  it('should allow missing optional fields', function(done) {
    const validation = {
      flags: 2147483648,
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30440220767031547C30519D5207540B70AC4DA39807CE99ADCF8FECF3342E7E7AC9209B02202CA6D25A3FFC233A553CE07DAA9AE152B986F6023FB96BA9E42BBCE744656DDC",
      signing_time: 514683328,
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }
    tmp_validations.handleValidation(validation)
    .then(() => {
      return hbase.getAllRows({
        table: 'validations_by_ledger'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      assert.strictEqual(rows[0].ledger_hash, validation.ledger_hash);
      assert.strictEqual(rows[0].amendments, undefined);
      assert.strictEqual(rows[0].base_fee, undefined);
      assert.strictEqual(rows[0].flags, validation.flags.toString());
      assert.strictEqual(rows[0].full, undefined);
      assert.strictEqual(rows[0].ledger_index, validation.ledger_index.toString());
      assert.strictEqual(rows[0].load_fee, validation.load_fee.toString());
      assert.strictEqual(rows[0].reserve_base, undefined);
      assert.strictEqual(rows[0].reserve_inc, undefined);
      assert.strictEqual(rows[0].signature, validation.signature);
      assert.strictEqual(rows[0].signing_time, validation.signing_time.toString());
      done();
    });
  });

  it('should verify signature using ephemeral public key', function(done) {
    const validation = {
      flags: 2147483649,
      full: true,
      ledger_hash: '714F7AD5BF42A38813B58D93A34E4B9771A85EEDECC3C5637368412B16CFC007',
      ledger_index: 7873719,
      signature: '304402206745C2B5ACC6C1B08FBE34D75D7C9581C8F0486123561B73C460417D9C35390C02202A1FB343E4583116139065CB3E23B051FF0DB48AA05DB2BEB9398F2896ABEEFA',
      signing_time: 522361117,
      validation_public_key: 'nHUkAWDR4cB8AgPg7VXMX6et8xRTQb2KJfgv1aBEXozwrawRKgMB',
      ephemeral_public_key: 'n9LYyd8eUVd54NQQWPAJRFPM1bghJjaf1rkdji2haF4zVjeAPjT2'
    }
    tmp_validations.handleValidation(validation)
    .then(() => {
      return hbase.getAllRows({
        table: 'validations_by_ledger'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].validation_public_key, validation.validation_public_key);
      assert.strictEqual(rows[0].ledger_hash, validation.ledger_hash);
      assert.strictEqual(rows[0].amendments, undefined);
      assert.strictEqual(rows[0].base_fee, undefined);
      assert.strictEqual(rows[0].flags, validation.flags.toString());
      assert.strictEqual(rows[0].full, 'true');
      assert.strictEqual(rows[0].ledger_index, validation.ledger_index.toString());
      assert.strictEqual(rows[0].load_fee, undefined);
      assert.strictEqual(rows[0].reserve_base, undefined);
      assert.strictEqual(rows[0].reserve_inc, undefined);
      assert.strictEqual(rows[0].signature, validation.signature);
      assert.strictEqual(rows[0].signing_time, validation.signing_time.toString());
      done();
    });
  });

  it('should require a validation public key', function(done) {
    tmp_validations.handleValidation({
      flags: 2147483648,
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30440220767031547C30519D5207540B70AC4DA39807CE99ADCF8FECF3342E7E7AC9209B02202CA6D25A3FFC233A553CE07DAA9AE152B986F6023FB96BA9E42BBCE744656DDC",
      signing_time: 514683328
    }).catch((err) => {
      assert.strictEqual(err, 'validation_public_key cannot be null');
      done();
    });
  });

  it('should require a ledger hash', function(done) {
    tmp_validations.handleValidation({
      flags: 2147483648,
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30440220767031547C30519D5207540B70AC4DA39807CE99ADCF8FECF3342E7E7AC9209B02202CA6D25A3FFC233A553CE07DAA9AE152B986F6023FB96BA9E42BBCE744656DDC",
      signing_time: 514683328,
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }).catch((err) => {
      assert.strictEqual(err, 'ledger_hash cannot be null');
      done();
    });
  });

  it('should require flags', function(done) {
    tmp_validations.handleValidation({
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30440220767031547C30519D5207540B70AC4DA39807CE99ADCF8FECF3342E7E7AC9209B02202CA6D25A3FFC233A553CE07DAA9AE152B986F6023FB96BA9E42BBCE744656DDC",
      signing_time: 514683328,
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }).catch((err) => {
      assert.strictEqual(err, 'flags cannot be null');
      done();
    });
  });

  it('should require a signature', function(done) {
    tmp_validations.handleValidation({
      flags: 2147483648,
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signing_time: 514683328,
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }).catch((err) => {
      assert.strictEqual(err, 'signature cannot be null');
      done();
    });
  });

  it('should require a signing time', function(done) {
    tmp_validations.handleValidation({
      flags: 2147483648,
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30440220767031547C30519D5207540B70AC4DA39807CE99ADCF8FECF3342E7E7AC9209B02202CA6D25A3FFC233A553CE07DAA9AE152B986F6023FB96BA9E42BBCE744656DDC",
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }).catch((err) => {
      assert.strictEqual(err, 'signing_time cannot be null');
      done();
    });
  });

  it('should require a valid signature', function(done) {
    tmp_validations.handleValidation({
      flags: 2147483648,
      ledger_hash: "41EE7EFCAFB912715D7D92D8C328747996ABFDF95A111667D1032F9334AFD45E",
      ledger_index: 5788323,
      load_fee: 256000,
      signature:"30450221009D9D65ADBD77D7D37DC7F40C7EE3249EBCF3033CE99B502EF376B9ECEB536DC80220564ACF514AA546ECF1CB04A4548CDBB24F6C2940A1DF36BDB0556DD9B64BBDE8",
      signing_time: 514683328,
      validation_public_key: "n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr"
    }).catch((err) => {
      assert.strictEqual(err, 'invalid signature');
      return Promise.delay(500)
    }).then(() => {
      return hbase.getAllRows({
        table: 'validations_by_ledger'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 0);
      done();
    });
  });
})

describe('validations import', function() {
  it('should save validations into hbase', function(done) {
    Promise.map(mockValidations, function(v) {
      return validations.handleValidation(v);
    }).then(function(resp) {
      assert.strictEqual(resp.length, mockValidations.length)
      assert.strictEqual(resp[0], '27D2720FDA393A076B62332A0535A734A42900B0DC47CC823CAE8F0B08298D97|n9KcuH7Y4q4SD3KoS5uXLhcDVvexpnYkwciCbcX131ehM5ek2BB6');
      assert.strictEqual(resp[1], '27D2720FDA393A076B62332A0535A734A42900B0DC47CC823CAE8F0B08298D97|n9LYyd8eUVd54NQQWPAJRFPM1bghJjaf1rkdji2haF4zVjeAPjT2');
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
      assert.strictEqual(body.reports.length, 5);
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
    var pubkey = 'n9MnXUt5Qcx3BuBYKJfS4fqSohgkT79NGjXnZeD9joKvP3A5RNGm';
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
    var pubkey = 'n9MnXUt5Qcx3BuBYKJfS4fqSohgkT79NGjXnZeD9joKvP3A5RNGm';
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
    var hash = '06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6';
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
      assert.strictEqual(body.validations.length, 5);
      body.validations.forEach(function(d) {
        assert.strictEqual(d.ledger_hash, hash);
      });
      done();
    });
  });

  it('should handle /ledgers/:hash/validations pagination correctly', function(done) {
    var hash = '06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.validations.length, 1);
      assert.equal(body.validations[0].signature, ref.validations[i].signature);
    }, done);
  });

  it('should include a link header when marker is present', function(done) {
    var hash = '06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6';
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?limit=1';
    var linkHeader = '<' + url +
      '&marker=06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6|n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr|';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers.link.substr(0,244), linkHeader);
      done();
    });
  });


  it('should get ledger validations in CSV format', function(done) {
    var hash = '06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6';
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
    var hash = '06851EAFACC3EAC2FE4AF6093215F63FFD8D3EF9709BED405057F84E1AB73FF6';
    var pubkey = 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr';
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
      assert.strictEqual(body.validators.length, 5);
      done();
    });
  });

  it('should get a single validator', function(done) {
    var pubkey = 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr';
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
      assert.strictEqual(body.validations.length, mockValidations.length);
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
    var pubkey = 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr';
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/validations';

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
