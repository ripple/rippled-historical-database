var Logger = require('../../lib/logger');
var log = new Logger({scope: 'gateways'});
var path = require('path');
var smoment = require('../../lib/smoment');
var validator = require('ripple-address-codec');
var assetPath = path.resolve(__dirname + '/../gateways/gatewayAssets/');
var currencies = path.resolve(__dirname + '/../gateways/currencyAssets/');
var fs = require('fs');
var files = fs.readdirSync(assetPath);
var assets = { };

// prepare assets
files.forEach(function(file) {
  var parts = file.split('.');
  var gateway = parts.shift();

  if (gateway) {
    if (!assets[gateway]) {
      assets[gateway] = [];
    }

    assets[gateway].push(parts.join('.'));
  }
});

/**
 * sortIssuers
 * sort by asset:featured:name
 */

function sortIssuers(a, b) {
  var one = (a.assets.length ? '0' : '1') + (a.featured ? '0' : '1') + a.name;
  var two = (b.assets.length ? '0' : '1') + (b.featured ? '0' : '1') + b.name;
  return (one >= two ? 1 : -1);
}

/**
 * normalize
 */

function normalize(name) {
  return name.toLowerCase().replace(/\W/g, '');
}

/**
 * isRippleAddress
 */

function isRippleAddress(d) {
  return /^r[1-9A-HJ-NP-Za-km-z]{25,34}$/.test(d);
}

/**
 * gateways
 * return information for all gatways
 * or a single gateway
 */

var Gateways = function(req, res) {
  var options;

  // cache for 1 hour
  res.setHeader('Cache-Control', 'max-age=3600');

  // single gateway
  if (req.params.gateway) {
    log.info('gateway:', req.params.gateway);

    if (isRippleAddress(req.params.gateway)) {
      options = {
        issuer: req.params.gateway
      }
    } else {
      options = {
        normalized_name: normalize(req.params.gateway)
      }
    }


    hbase.getGateways(options)
    .then(function(d) {
      if (d) {
        res.json(d);
      } else {
        res.status(404)
        .json({
          result: 'error',
          message: 'gateway not found.'
        });
      }
    })
    .catch(function(e) {
      log.error(e);
      res.status(500)
      .send({
        result: 'error',
        message: 'unable to retrieve gateway'
      });
    })

  // entire list
  } else {
    log.info('gateway list');

    hbase.getGateways()
    .then(filterIssuers)
    .then(addAssets)
    .then(sortByCurrency)
    .then(function(data) {
      res.json(data);
    })
    .catch(function(e) {
      log.error(e);
      console.log(e.stack);
      res.status(500)
      .send({
        result: 'error',
        message: 'unable to retrieve gateways'
      });
    })
  }

  function filterIssuers(rows) {
    return rows.filter(function(r) {
      return r.type === 'issuer';
    });
  }

  function addAssets(rows) {
    rows.forEach(function(r) {
      r.assets = assets[r.normalized_name] || [];
    });

    return rows;
  }

  function sortByCurrency(rows) {
    var data = {};

    rows.forEach(function(r) {
      if (!data[r.currency]) {
        data[r.currency] = [];
      }

      data[r.currency].push({
        name: r.name,
        account: r.address,
        featured: r.featured,
        start_date: r.start_date,
        assets: r.assets
      });
    });

    for (var key in data) {
      data[key] = data[key].sort(sortIssuers);
    }

    return data;
  }
};

/**
 * Assets
 * return gateway assets
 */

var Assets = function(req, res) {
  var filename = req.params.filename || 'logo.svg';
  var options = {};
  var name;

  log.info('asset:', req.params.gateway, filename);

  if (isRippleAddress(req.params.gateway)) {
    options = {
      issuer: req.params.gateway
    }
  } else {
    options = {
      normalized_name: normalize(req.params.gateway)
    }
  }

  hbase.getGateways(options)
  .then(function(gateway) {
    if (!gateway) {
      res.status(404)
      .json({
        result: 'error',
        message: 'gateway not found.'
      });

    } else {
      var name = normalize(gateway.name);
      res.setHeader('Cache-Control', 'max-age=3600');
      res.sendFile(assetPath + '/' + name + '.' + filename, null, function(err) {
        if (err) {
          log.error(err);
          res.status(err.status || 500)
          .json({
            result: 'error',
            message: 'asset not found.'
          });
        }
      });
    }
  });
};

/**
 * Currencies
 */

var Currencies = function (req, res, next) {
  var filename = (req.params.currencyAsset || 'default.svg').toLowerCase();

  log.info('currency:', filename);
  res.setHeader('Cache-Control', 'max-age=3600');
  res.sendFile(currencies + '/' + filename, null, function(err) {

    //send default svg if its not found
    if (err && err.status === 404) {
      res.sendFile(currencies + '/default.svg', null, function(err) {
        if (err) {
          res.status(500).send({
            result: 'error',
            message: 'server error.'
          });
        }
      });

    } else if (err) {
      log.error(err);
      res.status(err.status || 500)
      .send({
        result: 'error',
        message: 'server error.'
      });
    }
  });
};


module.exports = function(db) {
  hbase = db;

  return {
    Gateways: Gateways,
    Assets: Assets,
    Currencies: Currencies
  }
}

