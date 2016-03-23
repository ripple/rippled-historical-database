'use strict';

const config = require('../../config/import.config');
const request = require('request-promise');
const WebSocket = require('ws');
const Logger = require('../logger');
const log = new Logger({scope : 'validations etl'});
const colors = require('colors');
const PEER_PORT_REGEX = /51235/g;
const WS_PORT = '51233';
const manifests = require('./manifests')(config.get('hbase'));
const validations = require('./validations')(config.get('hbase'));

const connections = {};

/**
 * requestSubscribe
 */

function requestSubscribe(ws, altnet) {
  if (altnet) {
    ws.send(JSON.stringify({
      id: 222,
      command: 'server_info'
    }));
  }

  ws.send(JSON.stringify({
    id: 1,
    command: 'subscribe',
    streams: [
      'validations'
    ]
  }));

  ws.send(JSON.stringify({
    id: 2,
    command: 'subscribe',
    streams: [
      'manifests'
    ]
  }));
}

/**
 * subscribe
 */

function subscribe(rippled) {

  const ip = (rippled.altnet ? 'wss://' : 'ws://') +
    rippled.ipp.replace(PEER_PORT_REGEX, WS_PORT);

  // resubscribe to open connections
  if (connections[ip] &&
    connections[ip].readyState === WebSocket.OPEN) {
    try {
      requestSubscribe(connections[ip], rippled.altnet);
      return;

    } catch (e) {
      log.error(e.toString().red, ip.cyan);
      delete connections[ip];
    }

  } else if (connections[ip]) {
    connections[ip].close();
    delete connections[ip];
  }


  const ws = new WebSocket(ip);

  connections[ip] = ws;

  ws.public_key = rippled.public_key;

  // handle error
  ws.on('close', function() {
    log.info(this.url.cyan, 'closed'.yellow);
    if (this.url && connections[this.url]) {
      delete connections[this.url];
    }
  });

  // handle error
  ws.on('error', function(e) {
    if (this.url && connections[this.url]) {
      this.close();
      delete connections[this.url];
    }
  });

  // subscribe and save new connections
  ws.on('open', function() {
    if (this.url && connections[this.url]) {
      requestSubscribe(this, rippled.altnet);
    }
  });

  // handle messages
  ws.on('message', function(message) {
    const data = JSON.parse(message);

    if (data.type === 'validationReceived') {
      data.reporter_public_key = connections[this.url].public_key;

      // Store master key if validation is signed
      // by a known valid ephemeral key
      const master_public_key = manifests.getMasterKey(
        data.validation_public_key);
      if (master_public_key) {
        data.validation_public_key = master_public_key
      }

      validations.handleValidation(data);

    } else if (data.type === 'manifestReceived') {
      manifests.handleManifest(data);

    } else if (data.error === 'unknownStream') {
      delete connections[this.url];
      log.error(data.error, this.url.cyan);

    } else if (data.id === 222) {
      connections[this.url].public_key = data.result.info.pubkey_node;
    }
  });
}

/**
 * getRippleds
 */

function getRippleds(api_url) {
  return request.get({
    url: `${api_url}/rippleds`,
    json: true
  });
}

/**
 * subscribeToRippleds
 */

function subscribeToRippleds(rippleds) {
  const nRippled = rippleds.length.toString();
  const nConnections = Object.keys(connections).length.toString();

  log.info(('rippleds: ' + nRippled).yellow);
  log.info(('connections: ' + nConnections).yellow);

  // Subscribe to validation websocket subscriptions from rippleds
  for (const rippled of rippleds) {
    if (!rippled.ipp) {
      continue;
    }

    subscribe(rippled);
  }

  subscribe({
    ipp: 's.altnet.rippletest.net:51235',
    public_key: 'altnet',
    altnet: true
  });

  return connections;
}

/**
 * start
 */

function start() {
  getRippleds('http://54.201.174.180:1234')
  .then(subscribeToRippleds)
  .catch(e => {
    log.error(e.toString().red);
  });
}

manifests.start().then(() => {
  validations.start();

  // refresh connections
  // every minute
  setInterval(start, 60000);
  start();
});
