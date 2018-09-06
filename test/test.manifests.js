var config = require('../config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird');
var hbase = require('../lib/hbase');
var smoment = require('../lib/smoment');
var mockManifests = require('./mock/manifests.json')
var mockResponses = require('./mock/manifests.responses.json')
var Manifests = require('../lib/validations/manifests');
var _ = require('underscore');
var dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
var manifests;
var port = config.get('port') || 7111

describe('handleManifest', function(done) {
  beforeEach(function(done) {
    manifests = new Manifests();
    hbase.deleteAllRows({
      table: 'manifests_by_master_key'
    }).then(() => {
      hbase.deleteAllRows({
        table: 'manifests_by_validator'
      }).then(() => { done(); })
    });
  });

  it('should save manifests into hbase', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    manifests.handleManifest(manifest)
    .then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].master_public_key, manifest.master_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      assert.strictEqual(rows[0].signature, manifest.signature);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      done();
    });
  });

  it('should accept second signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: '3045022100b05e6738721100bf3eee37acab2ad60070bf9c7fa8e494f0d3f4de5eca291a220220677943ea5b1fcaadb71e6288a4a0982279c21bfbdcb3b0c3c26dffb84c27dd99',
      master_signature: '26e008edecbf7457f05fe569dbb6e0a6117da7d2e6903664647265429dae3acd5edc1fdbfdb792ff038086db46a98205d4d9ca73b8ef6a41ee42aa6ac3f5a70c'
    }).then(() => { done(); })
  });

  it('should require an ephemeral key', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'signing_key cannot be null');
      done();
    });
  });

  it('should accept revocation without ephemeral key or signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHB1PvPGSZhhNfdYDbwBmRmSWAEfd8YH97K9Bey82obyFh1nKDmq',
      seq: 4294967295,
      master_signature: 'A7D7E9C868874287B5888D5093DCAD8B8FF21BE306D9422C4DC7C143A37A2B961B490004892D6545A42A3A3291764B34A74D68FE1A0B22353E4F570F73C21401'
    }).then(() => { done(); })
  });

  it('should require a master key', function(done) {
    manifests.handleManifest({
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'master_key cannot be null');
      done();
    });
  });

  it('should require a sequence', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'seq cannot be null');
      done();
    });
  });

  it('should require a master signature or signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
    }).catch((err) => {
      assert.strictEqual(err, 'master signature and signature cannot be null');
      done();
    });
  });

  it('should require a valid signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
      signature: 'badsig'
    }).catch((err) => {
      assert.strictEqual(err, 'Manifest has invalid signature');
      done();
    });
  });

  it('should require a valid master signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: '3045022100b05e6738721100bf3eee37acab2ad60070bf9c7fa8e494f0d3f4de5eca291a220220677943ea5b1fcaadb71e6288a4a0982279c21bfbdcb3b0c3c26dffb84c27dd99',
      master_signature: 'badsig'
    }).catch((err) => {
      assert.strictEqual(err, 'Manifest has invalid signature');
      done();
    });
  });

  it('should not require a valid ephemeral signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: 'badsig',
      master_signature: '26e008edecbf7457f05fe569dbb6e0a6117da7d2e6903664647265429dae3acd5edc1fdbfdb792ff038086db46a98205d4d9ca73b8ef6a41ee42aa6ac3f5a70c'
    }).then(() => { done(); })
  });

  it('should cache new manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const old_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'
    const new_ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'

    manifests.handleManifest({
      signing_key: old_ephemeral_public_key,
      master_key: master_public_key,
      seq: 2,
      signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: new_ephemeral_public_key,
        master_key: master_public_key,
        seq: 4,
        signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, old_ephemeral_public_key);
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, new_ephemeral_public_key);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, '4');
      done();
    });
  });

  it('should not cache new stale manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const old_ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'
    const new_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'

    manifests.handleManifest({
      signing_key: old_ephemeral_public_key,
      master_key: master_public_key,
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: new_ephemeral_public_key,
        master_key: master_public_key,
        seq: 2,
        signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, old_ephemeral_public_key);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, old_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, '4');
      done();
    });
  });

  it('should not cache when first manifest revokes the master key', function(done) {

    manifests.handleManifest({
      signing_key: 'n9KVoK1g4NuSuMXqScpVhRVbBDNmLD8tPWeRKdewNVUN5F87YjwR',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4294967295,
      signature: '4d62df5d4cdc66b96a2fab58739515a636662cdf9d50d3b35ca7986293a72c3c0b5d518355274850e73e801628c5b6c7830546eab098f4e4548199bdc4993405'
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 0);
      done();
    });
  });

  it('should remove master key from cache when revoked', function(done) {

    manifests.handleManifest({
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: 'n9KVoK1g4NuSuMXqScpVhRVbBDNmLD8tPWeRKdewNVUN5F87YjwR',
        master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
        seq: 4294967295,
        signature: '4d62df5d4cdc66b96a2fab58739515a636662cdf9d50d3b35ca7986293a72c3c0b5d518355274850e73e801628c5b6c7830546eab098f4e4548199bdc4993405'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 0);
      done();
    });
  });

  it('should return cached master key', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    manifests.handleManifest(manifest)
    .then(() => {
      const master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, manifest.master_public_key);
      done();
    });
  })

  it('should store manifests from database in cache', function(done) {
    const manifest = {
      master_public_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      ephemeral_public_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      sequence: 4
    };

    const row = {
      table: 'manifests_by_master_key',
      rowkey: manifest.master_public_key,
      columns: {
        ephemeral_public_key: manifest.ephemeral_public_key,
        sequence: manifest.sequence
      }
    };

    hbase.putRow(row)
    .then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      var master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, undefined);
      return manifests.start();
    }).then(() => {
      master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, manifest.master_public_key);
      done();
    }).catch((e) => {
      assert.ifError(e);
    });
  });
});

describe('validator manifests endpoint', function() {

  before(function(done) {
    manifests = new Manifests();
    hbase.deleteAllRows({
      table: 'manifests_by_master_key'
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'manifests_by_validator'
      })
    }).then(() => {
      Promise.map(mockManifests, function(man) {
        return manifests.handleManifest(man)
      })
    }).then(() => { done(); })
  });

  it('should get validator manifest', function(done) {

    var pubkey = 'nHBV75zgMXCRHiuTMq6MdbcA6tBoSMWucTvHrnkQFW9gAXWoW15N'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 1)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      done()
    });
  });

  it('should get multiple validator manifests', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 2)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      _.isMatch(body.manifests[1], mockResponses[pubkey][1])
      done()
    });
  });

  it('should use limit and marker', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url + '?limit=1',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 1)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      request({
        url: url + '?marker=' + body.marker,
        json: true
      },
      function(err, res, body) {
        assert.ifError(err)
        assert.strictEqual(res.statusCode, 200)
        assert.strictEqual(body.manifests.length, 1)
        _.isMatch(body.manifests[0], mockResponses[pubkey][1])
        done()
      })
    });
  });

  it('should get validator revocation manifests', function(done) {
    var pubkey = 'nHUtR1DUzB5AbHFDTwByTF684SwvyDxDqwcsBavZR62VFESMCBHj'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests?descending=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 2)
      _.isMatch(body.manifests[0], mockResponses[pubkey][1])
      _.isMatch(body.manifests[1], mockResponses[pubkey][0])
      done()
    });
  });


  it('should get manifests in CSV format', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=manifests.csv')
      done()
    })
  })

  it('should get empty list if no manifest found', function(done) {
    var pubkey = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 0)
      done()
    })
  })
});
