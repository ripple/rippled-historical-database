var path = require('path');
var validator = require('ripple-address-codec');
var assetPath = path.resolve(__dirname + '/../gateways/gatewayAssets/');
var currencies = path.resolve(__dirname + '/../gateways/currencyAssets/');
var gatewayList = require('../gateways/gateways.json');
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

// add assets to gateway list
gatewayList.forEach(function(gateway) {
  gateway.normalized = normalize(gateway.name);
  gateway.assets = assets[gateway.normalized] || [];
});

// cached in memory since
// they will not change until restart
var gatewaysByCurrency = getGatewaysByCurrency();

// sort issuers for each currency
for (var key in gatewaysByCurrency) {
  gatewaysByCurrency[key].sort(sortIssuers);
}

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
 * getGatewaysByCurrency
 */

function getGatewaysByCurrency() {
  var results = { };
  gatewayList.forEach(function(gateway) {

    if (gateway.status !== 'active') {
      return;
    }

    gateway.accounts.forEach(function(acct) {
      for (var currency in acct.currencies) {
        if (!results[currency]) {
          results[currency] = [ ];
        }

        var g = {
          name: gateway.name,
          account: acct.address,
          featured: acct.currencies[currency].featured,
          label: acct.currencies[currency].label,
          assets: gateway.assets,
          startDate: gateway.startDate
        };

        results[currency].push(g);
      }
    });
  });

  return results;
}

/**
 * normalize
 */

function normalize(name) {
  return name.toLowerCase().replace(/\W/g, '');
}

/**
 * getGateway
 * get gateway details
 * from an issuer address
 */

function getGateway(identifier) {
  var gateway;
  var name;
  var address;
  var normalized;

  if (validator.isValidAddress(identifier)) {
    address = identifier;
  } else {
    name = normalize(identifier);
  }

  for (var i = 0; i < gatewayList.length; i++) {
    gateway = gatewayList[i];

    for (var j = 0; j < gateway.accounts.length; j++) {
      if (address && gateway.accounts[j].address === address) {
        return gateway;

      } else if (name) {
        normalized = normalize(gateway.name);

        if (name === normalized) {
          return gateway;
        }
      }
    }
  }
}

/**
 * gateways
 * return information for all gatways
 * or a single gateway
 */

var Gateways = function(req, res) {
  var address = req.params.gateway;
  var gateway;

  // single gateway
  if (address) {
    gateway = getGateway(address);
    if (gateway) {
      res.send(JSON.stringify(gateway, null));
    } else {
      res.status(404)
      .send({
        result: 'error',
        message: 'gateway not found.'
      });
    }

  // entire list
  } else {
    res.send(JSON.stringify(gatewaysByCurrency, null));
  }
};

/**
 * Assets
 * return gateway assets
 */

var Assets = function(req, res) {
  var address = req.params.gateway;
  var filename = req.params.filename || 'logo.svg';
  var gateway = getGateway(address);
  var name;

  if (!gateway) {
    res.status(400).send({
      result: 'error',
      message: 'gateway not found.'
    });
    return;
  }

  name = normalize(gateway.name);

  res.sendFile(assetPath + '/' + name + '.' + filename, null, function(err) {
    if (err) {
      res.status(err.status)
      .send({
        result: 'error',
        message: 'asset not found.'
      });
    }
  });
};

/**
 * Currencies
 */

var Currencies = function (req, res, next) {
  var filename = (req.params.currencyAsset || 'default.svg').toLowerCase();

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
      res.status(err.status).send({
        result: 'error',
        message: 'server error.'
      });
    }
  });
};


module.exports.Assets = Assets;
module.exports.Gateways = Gateways;
module.exports.Currencies = Currencies;
