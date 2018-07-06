'use strict';

const request = require('request-promise');
const Promise = require('bluebird');
const hbase = require('../hbase');
const Logger = require('../logger');
const log = new Logger({scope: 'geolocation'});

module.exports = config => {

  const cf = config.columnFamily || 'n';
  const table = config.table || 'node_state';

  function updateFromMaxmind(node) {
    const url = 'https://' +
      config.maxmind.user + ':' +
      config.maxmind.key + '@geoip.maxmind.com/geoip/v2.1/city/';

    return request.get({
      url: url + node.ip,
      json: true
    })
    .then(resp => {
      const subdivision = resp.subdivisions ?
            resp.subdivisions[resp.subdivisions.length - 1] : undefined;
      const city = resp.city ?
            resp.city.names.en : undefined;
      const postal_code = resp.postal ?
            resp.postal.code : undefined;
      const region = subdivision ?
            subdivision.names.en : undefined;
      const region_code = subdivision ?
            subdivision.iso_code : undefined;
      const country = resp.country ?
            resp.country.names.en : undefined;
      const country_code = resp.country ?
            resp.country.iso_code : undefined;
      const continent = resp.continent ?
            resp.continent.names.en : undefined;

      log.info(node.node_public_key.magenta,
               node.ip.grey,
               (city || '').blue,
               (region || '').cyan,
               (country || '').cyan.dim);

      const columns = {};

      columns[cf + ':lat'] = resp.location.latitude;
      columns[cf + ':long'] = resp.location.longitude;
      columns[cf + ':continent'] = continent;
      columns[cf + ':country'] = country;
      columns[cf + ':region'] = region;
      columns[cf + ':city'] = city;
      columns[cf + ':postal_code'] = postal_code;
      columns[cf + ':country_code'] = country_code;
      columns[cf + ':region_code'] = region_code;
      columns[cf + ':timezone'] = resp.location.time_zone;
      columns[cf + ':isp'] = resp.traits.isp;
      columns[cf + ':org'] = resp.traits.organization;
      columns[cf + ':domain'] = resp.traits.domain;

      return hbase.putRow({
        prefix: table,
        table: '',
        rowkey: node.node_public_key,
        columns: columns,
        removeEmptyColumns: true
      });
    });
  }

  function updateGeolocation(node) {
    const url = 'http://api.petabyet.com/geoip/' + node.ip;

    return request.get({
      url: url,
      json: true
    })
    .then(resp => {

      log.info(node.node_public_key.magenta,
               node.ip.grey,
               (resp.city || '').blue,
               (resp.region || '').cyan,
               (resp.country || '').cyan.dim);

      const columns = {};

      columns[cf + ':lat'] = resp.latitude;
      columns[cf + ':long'] = resp.longitude;
      columns[cf + ':country'] = resp.country;
      columns[cf + ':region'] = resp.region;
      columns[cf + ':city'] = resp.city;
      columns[cf + ':postal_code'] = resp.postal_code;
      columns[cf + ':country_code'] = resp.country_code;
      columns[cf + ':region_code'] = resp.region_code;
      columns[cf + ':timezone'] = resp.timezone;
      columns[cf + ':isp'] = resp.isp;

      return hbase.putRow({
        prefix: table,
        table: '',
        rowkey: node.node_public_key,
        columns: columns,
        removeEmptyColumns: true
      });
    });
  }

  return {
    geolocateNodes: function() {
      log.info('starting geolocation...'.yellow);
      return hbase.getTopologyNodes({
        limit: 10000,
      })
      .then(resp => resp.nodes)
      .then(nodes => {
        const list = nodes.filter(n => {
          return (/\./).test(n.ip);
        });

        log.info('found', list.length.toString().underline,
                'nodes with IP');

        return Promise.map(list, (node, i) => {

          return Promise.delay(i * 500)
          .then(() => {
            return config.maxmind ?
              updateFromMaxmind(node) : updateGeolocation(node);
          });
        });
      })
      .then(() => {
        log.info('geolocation complete'.green);
      });
    }
  };
};
